package ai.starlake.dbt.mapping

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.manifest.{DbtDoc, DbtProjectFile}
import ai.starlake.dbt.report.{BakedValue, EntryStatus, ImportReport, ReportEntry}
import org.apache.hadoop.fs.Path

/** Maps every object of the manifest and collects the report. The connection is the importer's. */
object DbtMapper {
  private val reportedKinds = Map(
    "exposures"       -> "exposure",
    "metrics"         -> "metric",
    "semantic_models" -> "semantic model",
    "saved_queries"   -> "saved query",
    "unit_tests"      -> "unit test"
  )

  def plan(ctx: DbtMappingContext, project: Option[DbtProjectFile])(implicit
    settings: Settings
  ): ImportPlan = {
    val manifest = ctx.manifest
    val claimed = scala.collection.mutable.Map.empty[String, String]
    val models = firstOfEachFile(
      manifest.nodesOfType("model").map(DbtModelMapper.map(_, ctx)),
      claimed
    )(_.entry, _.outputs.map(_.path))
    val seeds =
      firstOfEachFile(manifest.nodesOfType("seed").map(DbtSeedMapper.map(_, ctx)), claimed)(
        _.entry,
        s =>
          List(
            new Path(DatasetArea.load, s"${s.domain}/${s.table.name}.sl.yml"),
            new Path(DatasetArea.incoming(s.domain), s.csv.getFileName.toString)
          )
      )
    val sources = firstOfEachFile(manifest.sources.map(DbtSourceMapper.map(_, ctx)), claimed)(
      _.entry,
      s => s.table.toList.map(t => new Path(DatasetArea.external, s"${s.domain}/${t.name}.sl.yml"))
    )
    val mappedModels = models.collect { case Right(m) => m }
    val mappedSeeds = seeds.collect { case Right(s) => s }
    val mappedSources = sources.collect { case Right(s) => s }

    val converted = (mappedModels.map(_.entry.dbtId) ++ mappedSeeds.map(_.entry.dbtId)).toSet
    val sourceToSeed = mappedSources.flatMap(s => s.seedId.map(s.entry.dbtId -> _)).toMap
    val tests = DbtTestMapper.map(manifest.nodesOfType("test"), converted, sourceToSeed, ctx)

    val tasks = mappedModels.map { m =>
      m.copy(task = m.task.copy(expectations = tests.expectations.getOrElse(m.entry.dbtId, Nil)))
    }
    val tables = mappedSeeds.map { s =>
      s.copy(table = s.table.copy(expectations = tests.expectations.getOrElse(s.entry.dbtId, Nil)))
    }
    val allDocs = manifest.docs.filterNot(_.packageName == "dbt")
    val unsafeDocs = allDocs.flatMap(d =>
      DbtText.unsafeField("package" -> d.packageName, "name" -> d.name).map(d -> _)
    )
    val docs = allDocs.filterNot(d => unsafeDocs.exists(_._1 eq d))

    val (seedOutputs, seedNotes) = DbtSeedMapper.plan(tables, ctx.connectionRef)
    val (sourceOutputs, sourceNotes) = DbtSourceMapper.plan(mappedSources)
    val outputs =
      tasks.flatMap(_.outputs) ++
      seedOutputs ++
      sourceOutputs ++
      tests.output.toList ++
      docs.map(docOutput)

    val reported =
      manifest.nodes.filter(n => n.resourceType == "snapshot" || n.resourceType == "analysis").map {
        n =>
          val reason =
            if (n.resourceType == "snapshot") "snapshots are not imported yet"
            else "analyses are not imported"
          ReportEntry(n.resourceType, n.uniqueId, None, EntryStatus.ReportedOnly(reason))
      } ++
      manifest.disabled.map { n =>
        ReportEntry(
          n.resourceType,
          n.uniqueId,
          Some(ctx.relations.ofNode(n).render),
          EntryStatus.NotImported("enabled: false")
        )
      } ++
      manifest.reportedOnly.map { case (kind, id) =>
        ReportEntry(
          reportedKinds.getOrElse(kind, kind),
          id,
          None,
          EntryStatus.ReportedOnly("no Starlake equivalent")
        )
      }

    val entries =
      models.map(_.fold(identity, _.entry)) ++
      seeds.map(_.fold(identity, _.entry)) ++
      sources.map(_.fold(identity, _.entry)) ++
      tests.entries ++
      docs.map(d =>
        ReportEntry(
          "doc",
          d.uniqueId,
          Some(s"metadata/docs/dbt/${d.packageName}/${d.name}.md"),
          EntryStatus.Converted
        )
      ) ++
      unsafeDocs.map { case (d, reason) =>
        ReportEntry("doc", d.uniqueId, None, EntryStatus.Skipped(reason))
      } ++
      reported

    val general = project.toList.flatMap { p =>
      p.onRunStart.map(s => s"on-run-start is not imported: $s") ++
      p.onRunEnd.map(s => s"on-run-end is not imported: $s")
    } ++ seedNotes ++ sourceNotes ++
      ctx.jinja.definitionErrors.map(e => s"dbt macro not defined: $e")

    ImportPlan(
      outputs,
      None,
      ImportReport(
        entries,
        baked(mappedModels.map(m => m.entry.target.getOrElse(m.entry.dbtId) -> m.vars)),
        baked(mappedModels.map(m => m.entry.target.getOrElse(m.entry.dbtId) -> m.envVars)),
        general,
        BakedValue.merge(
          mappedModels.flatMap(m =>
            m.warehouseReads.map(r =>
              BakedValue(r.what, r.result, List(m.entry.target.getOrElse(m.entry.dbtId)))
            )
          )
        )
      )
    )
  }

  /** Two objects that map to the same file (the same schema and alias in two databases, say): the
    * first one is kept, the next ones are skipped with a reason that names it
    *
    * @param claimed
    *   the files of the objects kept so far, with the dbt object that writes each
    */
  private def firstOfEachFile[T](
    mapped: List[Either[ReportEntry, T]],
    claimed: scala.collection.mutable.Map[String, String]
  )(entry: T => ReportEntry, paths: T => List[Path]): List[Either[ReportEntry, T]] =
    mapped.map {
      case Right(t) =>
        val files = paths(t).map(_.toString)
        files.find(claimed.contains) match {
          case Some(file) =>
            Left(
              entry(t).copy(
                status = EntryStatus.Skipped(s"writes the same file as ${claimed(file)}: $file"),
                notes = Nil
              )
            )
          case None =>
            files.foreach(f => claimed.put(f, entry(t).dbtId))
            Right(t)
        }
      case skipped => skipped
    }

  private def baked(uses: List[(String, Map[String, String])]): List[BakedValue] =
    uses
      .flatMap { case (user, values) => values.map { case (name, value) => (name, value, user) } }
      .groupBy(u => (u._1, u._2))
      .toList
      .sortBy(_._1)
      .map { case ((name, value), us) => BakedValue(name, value, us.map(_._3).distinct.sorted) }

  private def docOutput(doc: DbtDoc)(implicit settings: Settings): DbtOutput =
    FileOutput(
      new Path(DatasetArea.metadata, s"docs/dbt/${doc.packageName}/${doc.name}.md"),
      s"{% docs ${doc.name} %}\n${doc.blockContents.trim}\n{% enddocs %}\n"
    )
}
