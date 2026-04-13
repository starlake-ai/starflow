package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.schema.ProjectCompareConfig
import ai.starlake.schema.handlers.SchemaHandler
import org.apache.hadoop.fs.Path

object Project {
  def compare(config: ProjectCompareConfig)(implicit settings: Settings): ProjectDiff =
    compare(new Path(config.path1), new Path(config.path2))

  def compare(project1Path: Path, project2Path: Path)(implicit settings: Settings): ProjectDiff = {
    val settings1 =
      settings.copy(appConfig =
        settings.appConfig.copy(metadata = new Path(project1Path, "metadata").toString)
      )

    val settings2 =
      settings.copy(appConfig =
        settings.appConfig.copy(metadata = new Path(project2Path, "metadata").toString)
      )

    val schemaHandler1 = settings1.schemaHandler(reload = true)
    val schemaHandler2 = settings2.schemaHandler(reload = true)

    ProjectDiff(
      project1Path.toString,
      project2Path.toString,
      domainsDiff(schemaHandler1, schemaHandler2),
      jobsDiff(schemaHandler1, schemaHandler2)
    )
  }

  private def domainsDiff(
    schemaHandler1: SchemaHandler,
    schemaHandler2: SchemaHandler
  ): DomainsDiff = {
    val p1Domains =
      schemaHandler1.domains(raw = true, reload = true)

    val p2Domains =
      schemaHandler2.domains(raw = true, reload = true)

    val (addedDomains, deletedDomains, existingCommonDomains) =
      AnyRefDiff.partitionNamed(p1Domains, p2Domains)

    val commonDomains: List[(DomainInfo, DomainInfo)] = existingCommonDomains.map { domain =>
      (
        domain,
        p2Domains
          .find(_.name.toLowerCase() == domain.name.toLowerCase())
          .getOrElse(throw new IllegalStateException("Should not happen"))
      )
    }

    val updatedDomainsDiff: List[DomainDiff] = commonDomains.flatMap { case (existing, incoming) =>
      DomainInfo.compare(existing, incoming).toOption
    }
    DomainsDiff(addedDomains.map(_.name), deletedDomains.map(_.name), updatedDomainsDiff)
  }

  private def jobsDiff(
    schemaHandler1: SchemaHandler,
    schemaHandler2: SchemaHandler
  ): JobsDiff = {

    val p1Jobs = schemaHandler1.jobs(reload = true)
    val p2Jobs = schemaHandler2.jobs(reload = true)

    val (addedJobs, deletedJobs, existingCommonJobs) =
      AnyRefDiff.partitionNamed(p1Jobs, p2Jobs)

    val commonJobs: List[(AutoJobInfo, AutoJobInfo)] = existingCommonJobs.map { job =>
      (
        job,
        p2Jobs
          .find(_.name.toLowerCase() == job.name.toLowerCase())
          .getOrElse(throw new IllegalStateException("Should not happen"))
      )
    }

    val updatedJobsDiff: List[TransformsDiff] = commonJobs.flatMap { case (existing, incoming) =>
      AutoJobInfo.compare(existing, incoming).toOption
    }
    JobsDiff(addedJobs.map(_.name), deletedJobs.map(_.name), updatedJobsDiff)
  }
}
