package ai.starlake.job.site.stats

import ai.starlake.job.site.SiteColumn

import java.util.Locale

object ColumnDiffer {

  /** Compares top-level declared columns with the warehouse columns. Empty when nothing is
    * declared.
    */
  def diff(declared: List[SiteColumn], actual: List[ColumnStat], engine: String): List[ColumnDiff] =
    if (declared.isEmpty) Nil
    else {
      val actualByName = actual.map(c => c.name.toLowerCase(Locale.ROOT) -> c).toMap
      val declaredNames =
        declared.map(c => c.rename.getOrElse(c.name).toLowerCase(Locale.ROOT)).toSet
      val fromDeclared = declared.map { c =>
        val name = c.rename.getOrElse(c.name)
        actualByName.get(name.toLowerCase(Locale.ROOT)) match {
          case None => ColumnDiff(name, c.sqlType, None, "missingInWarehouse")
          case Some(a) =>
            val status = c.sqlType match {
              case Some(d) if !TypeNormalizer.same(engine, d, a.`type`) => "mismatch"
              case _                                                    => "match"
            }
            ColumnDiff(name, c.sqlType, Some(a.`type`), status)
        }
      }
      val extras = actual
        .filterNot(a => declaredNames.contains(a.name.toLowerCase(Locale.ROOT)))
        .map(a => ColumnDiff(a.name, None, Some(a.`type`), "extraInWarehouse"))
      fromDeclared ++ extras
    }
}
