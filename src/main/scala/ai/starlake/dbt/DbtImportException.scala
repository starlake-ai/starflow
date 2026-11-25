package ai.starlake.dbt

/** A dbt project, manifest or option that the import cannot read: a usage error (exit code 1) */
final class DbtImportException(message: String) extends IllegalArgumentException(message)

/** Why one dbt object is not converted: the import goes on with the others */
final case class DbtSkip(reason: String) extends RuntimeException(reason)
