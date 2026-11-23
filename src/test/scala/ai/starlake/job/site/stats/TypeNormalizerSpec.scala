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

  it should "compare the base type only when either side has no parameters" in {
    same("postgresql", "VARCHAR(8000)", "text") shouldBe true
    same("postgresql", "NUMERIC(38,9)", "numeric") shouldBe true
    same("duckdb", "DECIMAL", "DECIMAL(18,3)") shouldBe true
  }

  "normalize" should "read a precision placed before the time zone" in {
    TypeNormalizer.normalize("postgresql", "timestamp(6) with time zone") shouldBe
    NormalizedType("TIMESTAMPTZ", List("6"))
    TypeNormalizer.normalize("postgresql", "timestamp(3) without time zone") shouldBe
    NormalizedType("TIMESTAMP", List("3"))
    TypeNormalizer.normalize("postgresql", "time(3) without time zone") shouldBe
    NormalizedType("TIME", List("3"))
  }

  it should "not depend on the default locale" in {
    val default = java.util.Locale.getDefault
    try {
      java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"))
      TypeNormalizer.normalize("postgresql", "int") shouldBe NormalizedType("INTEGER", Nil)
      TypeNormalizer.normalize("snowflake", "timestamp_ltz") shouldBe
      NormalizedType("TIMESTAMPTZ", Nil)
    } finally java.util.Locale.setDefault(default)
  }

  /** Starlake ddlMapping output (declared) versus the spelling the warehouse reports (actual). */
  private val matching = List(
    ("duckdb", "DECIMAL", "DECIMAL(18,3)"),
    ("duckdb", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"),
    ("postgresql", "varchar(8000)", "character varying(8000)"),
    ("postgresql", "DECIMAL", "numeric(18,2)"),
    ("postgresql", "TIMESTAMP", "timestamp without time zone"),
    ("postgresql", "TIMESTAMPTZ", "timestamp(6) with time zone"),
    ("postgresql", "INT", "integer"),
    ("snowflake", "INT", "NUMBER(38,0)"),
    ("snowflake", "STRING", "TEXT"),
    ("snowflake", "TIMESTAMP", "TIMESTAMP_NTZ(9)"),
    ("bigquery", "INT64", "INT64"),
    ("bigquery", "STRING", "STRING"),
    ("bigquery", "NUMERIC", "NUMERIC"),
    ("bigquery", "TIMESTAMP", "TIMESTAMP"),
    ("redshift", "BIGINT", "bigint"),
    ("redshift", "TEXT", "character varying(256)"),
    ("spark", "BIGINT", "bigint"),
    ("spark", "STRING", "string"),
    ("spark", "DECIMAL(38,9)", "decimal(38,9)"),
    // declaredSqlType on postgresql (Spark JDBC dialect types) versus information_schema
    ("postgresql", "FLOAT8", "double precision"),
    ("postgresql", "TEXT", "text"),
    ("postgresql", "NUMERIC(38,9)", "numeric(38,9)"),
    ("postgresql", "TIMESTAMP WITH TIME ZONE", "timestamp with time zone")
  )

  private val mismatching = List(
    ("postgresql", "varchar(10)", "character varying(20)"),
    ("bigquery", "INT64", "STRING"),
    ("bigquery", "TIMESTAMP", "DATETIME"),
    ("snowflake", "INT", "NUMBER(18,2)")
  )

  "same" should "match Starlake ddlMapping outputs with real warehouse spellings" in {
    matching.foreach { case (engine, declared, actual) =>
      withClue(s"$engine: $declared vs $actual") {
        same(engine, declared, actual) shouldBe true
      }
    }
  }

  it should "keep real mismatches" in {
    mismatching.foreach { case (engine, declared, actual) =>
      withClue(s"$engine: $declared vs $actual") {
        same(engine, declared, actual) shouldBe false
      }
    }
  }
}
