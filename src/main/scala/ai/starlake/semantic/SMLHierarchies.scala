package ai.starlake.semantic

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.jdk.CollectionConverters._

/** Parses and validates the optional `hierarchies` list of a semantic model table. Invalid
  * hierarchies are logged and dropped: they never fail the export.
  */
private[semantic] object SMLHierarchies extends LazyLogging {

  import SemanticModelOps._

  sealed trait Hierarchy {
    def name: String
    def description: Option[String]
  }

  /** Levels coarse to fine. `field` and `key` hold field names in their declared case. */
  case class GeneralLevel(field: String, key: List[String])

  case class GeneralHierarchy(
    name: String,
    description: Option[String],
    levels: List[GeneralLevel]
  ) extends Hierarchy

  case class TimeHierarchy(
    name: String,
    description: Option[String],
    time: String,
    units: List[String]
  ) extends Hierarchy

  val TimeUnits: List[String] = List("year", "quarter", "month", "day")

  /** @param fields
    *   dimension, time_dimension and fact names of the table
    * @param timeFields
    *   time_dimension names of the table
    * @return
    *   the valid hierarchies in declaration order and the number of skipped ones
    */
  def parse(
    table: JsonNode,
    fields: List[String],
    timeFields: List[String]
  ): (List[Hierarchy], Int) = {
    val tableName = table.path("name").asText()
    elems(table, "hierarchies").foldLeft((List.empty[Hierarchy], 0)) {
      case ((valid, skipped), node) =>
        val parsed = parseOne(node, fields, timeFields).flatMap { h =>
          if (valid.exists(_.name.equalsIgnoreCase(h.name)))
            Left("a hierarchy with the same name is already declared")
          else Right(h)
        }
        parsed match {
          case Right(h) => (valid :+ h, skipped)
          case Left(reason) =>
            logger.warn(
              s"Table '$tableName': hierarchy '${text(node, "name").getOrElse("")}' skipped, $reason"
            )
            (valid, skipped + 1)
        }
    }
  }

  private def parseOne(
    node: JsonNode,
    fields: List[String],
    timeFields: List[String]
  ): Either[String, Hierarchy] = {
    val name = text(node, "name").getOrElse("")
    val description = text(node, "description")
    def resolve(field: String, among: List[String]): Either[String, String] =
      among.find(_.equalsIgnoreCase(field.trim)).toRight(s"'$field' is not a field of the table")

    if (name.isEmpty) Left("it has no name")
    else if (node.has("time")) {
      val units =
        if (node.path("levels").isArray)
          node.get("levels").elements().asScala.map(_.asText().trim.toLowerCase).toList
        else Nil
      for {
        time <- timeFields
          .find(_.equalsIgnoreCase(node.path("time").asText().trim))
          .toRight(s"'${node.path("time").asText()}' is not a time_dimension of the table")
        _ <- if (units.isEmpty) Left("it has no levels") else Right(())
        _ <-
          if (units.distinct.size == units.size && units == TimeUnits.filter(units.contains))
            Right(())
          else Left(s"levels must be an ordered subset of ${TimeUnits.mkString(", ")}")
      } yield TimeHierarchy(name, description, time, units)
    } else {
      val levelNodes = elems(node, "levels")
      if (levelNodes.isEmpty) Left("it has no levels")
      else {
        val levels = levelNodes.foldLeft[Either[String, List[GeneralLevel]]](Right(Nil)) {
          (acc, level) =>
            for {
              done  <- acc
              field <- resolve(level.path("field").asText(), fields)
              key <- elems(level, "key") match {
                case Nil => Right(List(field))
                case keys =>
                  keys.foldLeft[Either[String, List[String]]](Right(Nil)) { (keyAcc, k) =>
                    for {
                      resolved <- keyAcc
                      column   <- resolve(k.asText(), fields)
                    } yield resolved :+ column
                  }
              }
            } yield done :+ GeneralLevel(field, key)
        }
        levels.flatMap { ls =>
          if (ls.map(_.field.toLowerCase).distinct.size != ls.size)
            Left("a field is used by more than one level")
          else Right(GeneralHierarchy(name, description, ls))
        }
      }
    }
  }
}
