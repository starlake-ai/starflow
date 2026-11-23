package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypeNormalizerSpec extends AnyFlatSpec with Matchers {
  import TypeNormalizer.same

  "normalize" should "upper-case, collapse spaces and split params" in {
    TypeNormalizer.normalize("postgresql", " character  varying(255) ") shouldBe
      NormalizedType("VARCHAR", List("255"))
    TypeNormalizer.normalize("duckdb", "DECIMAL(38, 9)") shouldBe
      NormalizedType("DECIMAL", List("38", "9"))
    TypeNormalizer.normalize("spark", "array<string>") shouldBe NormalizedType("ARRAY<STRING>", Nil)
  }

  "same" should "apply common aliases" in {
    same("postgresql", "BIGINT", "int8") shouldBe true
    same("postgresql", "INT", "integer") shouldBe true
    same("postgresql", "DOUBLE", "double precision") shouldBe true
    same("postgresql", "TIMESTAMP", "timestamp without time zone") shouldBe true
    same("postgresql", "TIMESTAMPTZ", "timestamp with time zone") shouldBe true
    same("redshift", "VARCHAR", "character varying(256)") shouldBe true
    same("postgresql", "BOOLEAN", "bool") shouldBe true
  }

  it should "compare precision only when the declared type has one" in {
    same("postgresql", "DECIMAL", "numeric(18,2)") shouldBe true
    same("postgresql", "DECIMAL(18,2)", "numeric(18,2)") shouldBe true
    same("postgresql", "DECIMAL(38,9)", "numeric(18,2)") shouldBe false
    same("postgresql", "VARCHAR(10)", "character varying(20)") shouldBe false
  }

  it should "handle BigQuery aliases" in {
    same("bigquery", "INT64", "INTEGER") shouldBe true
    same("bigquery", "INTEGER", "INT64") shouldBe true
    same("bigquery", "FLOAT64", "FLOAT64") shouldBe true
    same("bigquery", "STRING", "STRING(20)") shouldBe true
    same("bigquery", "NUMERIC", "DECIMAL") shouldBe true
    same("bigquery", "TIMESTAMP", "DATETIME") shouldBe false
  }

  it should "handle Snowflake integers as NUMBER(38,0)" in {
    same("snowflake", "BIGINT", "NUMBER(38,0)") shouldBe true
    same("snowflake", "INTEGER", "NUMBER(38,0)") shouldBe true
    same("snowflake", "DECIMAL(38,9)", "NUMBER(18,2)") shouldBe false
    same("snowflake", "STRING", "TEXT(16777216)") shouldBe true
    same("snowflake", "DOUBLE", "FLOAT") shouldBe true
  }

  it should "handle engines where FLOAT is 4 bytes" in {
    same("duckdb", "REAL", "FLOAT") shouldBe true
    same("duckdb", "DOUBLE", "FLOAT") shouldBe false
    same("spark", "FLOAT", "float") shouldBe true
    same("postgresql", "DOUBLE", "float") shouldBe true
  }

  it should "map Spark timestamps" in {
    same("spark", "TIMESTAMP", "timestamp") shouldBe true
    same("spark", "TIMESTAMP_NTZ", "timestamp_ntz") shouldBe true
    same("spark", "TIMESTAMP", "timestamp_ntz") shouldBe false
    same("spark", "STRING", "string") shouldBe true
  }

  it should "report different families as different" in {
    same("postgresql", "BIGINT", "character varying") shouldBe false
    same("duckdb", "DATE", "TIMESTAMP") shouldBe false
  }
}
