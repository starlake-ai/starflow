package ai.starlake.semantic

import ai.starlake.utils.YamlSerde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SMLHierarchiesSpec extends AnyFlatSpec with Matchers {

  import SMLHierarchies._

  private def table(yaml: String) = YamlSerde.mapper.readTree(yaml.stripMargin)
  private val fields = List("country", "region", "city", "customer_id", "signup_date")
  private val timeFields = List("signup_date")

  "SMLHierarchies.parse" should "parse general and time hierarchies" in {
    val t = table(
      """name: customers
        |hierarchies:
        |  - name: geography
        |    description: Where customers live
        |    levels:
        |      - field: Country
        |      - field: region
        |        key: [country, region]
        |  - name: signup_calendar
        |    time: signup_date
        |    levels: [year, month]
        |"""
    )
    parse(t, fields, timeFields) shouldBe ((
      List(
        GeneralHierarchy(
          "geography",
          Some("Where customers live"),
          List(
            GeneralLevel("country", List("country")),
            GeneralLevel("region", List("country", "region"))
          )
        ),
        TimeHierarchy("signup_calendar", None, "signup_date", List("year", "month"))
      ),
      0
    ))
  }

  it should "skip invalid hierarchies and count them" in {
    val t = table(
      """name: customers
        |hierarchies:
        |  - name: unknown_field
        |    levels: [{field: planet}]
        |  - name: bad_key
        |    levels: [{field: city, key: [galaxy]}]
        |  - name: repeated_level
        |    levels: [{field: city}, {field: CITY}]
        |  - name: no_levels
        |    levels: []
        |  - levels: [{field: city}]
        |  - name: not_time
        |    time: city
        |    levels: [year]
        |  - name: unordered
        |    time: signup_date
        |    levels: [month, year]
        |  - name: bad_unit
        |    time: signup_date
        |    levels: [week]
        |  - name: geo
        |    levels: [{field: city}]
        |  - name: GEO
        |    levels: [{field: country}]
        |"""
    )
    parse(t, fields, timeFields) shouldBe ((
      List(GeneralHierarchy("geo", None, List(GeneralLevel("city", List("city"))))),
      9
    ))
  }

  it should "return nothing for a table without hierarchies" in {
    parse(table("name: t\n"), fields, timeFields) shouldBe ((Nil, 0))
  }
}
