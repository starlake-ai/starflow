package ai.starlake.job.site.stats

import Rows._

object InfoSchemaMapping {
  private val decimalTypes = Set("numeric", "decimal", "number")
  private val charTypes =
    Set(
      "character varying",
      "varchar",
      "character",
      "char",
      "nvarchar",
      "nchar",
      "text",
      "string",
      "bpchar"
    )

  def formatType(
    dataType: String,
    charLen: Option[Long],
    precision: Option[Long],
    scale: Option[Long]
  ): String = {
    val dt = dataType.trim
    val lower = dt.toLowerCase
    if (dt.contains("(")) dt
    else if (decimalTypes.contains(lower) && precision.isDefined)
      s"$dt(${precision.get},${scale.getOrElse(0L)})"
    else if (charTypes.contains(lower) && charLen.isDefined) s"$dt(${charLen.get})"
    else dt
  }

  def tableType(raw: Option[String]): String = raw.map(_.toUpperCase) match {
    case Some(t) if t.contains("VIEW")                              => "VIEW"
    case Some(t) if t.contains("EXTERNAL") || t.contains("FOREIGN") => "EXTERNAL"
    case _                                                          => "TABLE"
  }

  def columns(rows: List[Row]): Map[String, List[ColumnStat]] =
    rows
      .flatMap { r =>
        for {
          table  <- str(r, "table_name")
          column <- str(r, "column_name")
        } yield table.toLowerCase -> ColumnStat(
          column,
          formatType(
            str(r, "data_type").getOrElse(""),
            long(r, "character_maximum_length"),
            long(r, "numeric_precision"),
            long(r, "numeric_scale")
          ),
          nullable = !str(r, "is_nullable").exists(_.equalsIgnoreCase("NO"))
        )
      }
      .groupBy(_._1)
      .map { case (k, v) => k -> v.map(_._2) }

  def merge(
    tableRows: List[Row],
    statsRows: List[Row],
    columnRows: List[Row],
    estimated: Boolean
  ): Map[String, RawTableStats] = {
    val cols = columns(columnRows)
    val statsByName = (tableRows ++ statsRows)
      .flatMap(r => str(r, "table_name").map(_.toLowerCase -> r))
      .groupBy(_._1)
      .map { case (k, v) => k -> v.map(_._2) }
    tableRows.flatMap { r =>
      str(r, "table_name").map { name =>
        val key = name.toLowerCase
        val all = statsByName.getOrElse(key, Nil)
        val rowCount = all.flatMap(long(_, "row_count")).find(_ >= 0)
        key -> RawTableStats(
          exists = true,
          tableType = Some(tableType(str(r, "table_type"))),
          format = None,
          rowCount = rowCount,
          rowCountEstimated = estimated && rowCount.isDefined,
          bytes = all.flatMap(long(_, "bytes")).headOption,
          lastModified = all.flatMap(instant(_, "last_modified")).headOption,
          columns = cols.getOrElse(key, Nil)
        )
      }
    }.toMap
  }
}
