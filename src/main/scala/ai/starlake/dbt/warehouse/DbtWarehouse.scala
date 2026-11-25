package ai.starlake.dbt.warehouse

import ai.starlake.dbt.DbtSkip

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** A table as the import names it: database only when it is not the project's default */
final case class WarehouseRelation(database: Option[String], schema: String, identifier: String) {
  def render: String = (database.toList :+ schema :+ identifier).mkString(".")
}

/** What a render read from the warehouse, for the report */
final case class WarehouseRead(what: String, result: String)

/** The warehouse that the import's macros read. A Failure(DbtSkip) skips the model. */
trait DbtWarehouse {
  def name: String

  /** True when there is no warehouse behind this one: every call skips */
  def offline: Boolean = false

  /** Reads at most `maxRows` rows: a caller that wants to know about an overflow asks for one more
    */
  def query(sql: String, maxRows: Int): Try[DbtTable]
  def query(sql: String): Try[DbtTable] = query(sql, Int.MaxValue)
  def columns(relation: WarehouseRelation): Try[List[DbtWarehouseColumn]]
  def relation(relation: WarehouseRelation): Try[Option[WarehouseRelation]]
}

/** Without --introspect: every call skips the model, as before introspection */
final class OfflineWarehouse extends DbtWarehouse {
  def name: String = "offline"
  override def offline: Boolean = true
  def query(sql: String, maxRows: Int): Try[DbtTable] = OfflineWarehouse.skip("run_query")
  def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] =
    OfflineWarehouse.skip("adapter.get_columns_in_relation")
  def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] =
    OfflineWarehouse.skip("adapter.get_relation")
}

object OfflineWarehouse {
  def reason(call: String): String = s"needs the warehouse at import ($call)"
  def skip[T](call: String): Try[T] = Failure(DbtSkip(reason(call)))
}

/** The read-only guard, the row limit, and one answer per statement and per relation for the whole
  * import (both renders of an incremental model, every model reading the same source)
  */
/** Used by one import at a time: renders are sequential, the caches are not synchronized */
final class CachingWarehouse(underlying: DbtWarehouse, maxRows: Int) extends DbtWarehouse {
  private val queries = mutable.Map.empty[String, Try[DbtTable]]
  private val columnsOf = mutable.Map.empty[WarehouseRelation, Try[List[DbtWarehouseColumn]]]
  private val relations = mutable.Map.empty[WarehouseRelation, Try[Option[WarehouseRelation]]]

  def name: String = underlying.name
  override def offline: Boolean = underlying.offline

  def query(sql: String, rows: Int): Try[DbtTable] = query(sql).filter(_.size <= rows)

  override def query(sql: String): Try[DbtTable] = {
    val key = sql.trim.stripSuffix(";").trim
    queries.getOrElseUpdate(
      key,
      ReadOnlyGuard.check(key) match {
        case Left(reason) => Failure(DbtSkip(reason))
        case Right(_) =>
          underlying.query(key, maxRows + 1).recoverWith(asSkip).flatMap { t =>
            if (t.size > maxRows)
              Failure(
                DbtSkip(s"run_query returned more than $maxRows rows (--introspect-max-rows)")
              )
            else Success(t)
          }
      }
    )
  }

  def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] =
    columnsOf.getOrElseUpdate(r, underlying.columns(r).recoverWith(asSkip))

  def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] =
    relations.getOrElseUpdate(r, underlying.relation(r).recoverWith(asSkip))

  private val asSkip: PartialFunction[Throwable, Try[Nothing]] = {
    case s: DbtSkip => Failure(s)
    case e =>
      Failure(
        DbtSkip(
          Option(e.getMessage)
            .flatMap(_.linesIterator.nextOption())
            .filter(_.nonEmpty)
            .getOrElse(e.toString)
        )
      )
  }
}
