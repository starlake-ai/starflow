package ai.starlake.utils

/** Base exception for Starlake-specific errors. */
class StarlakeException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** Thrown when a required resource (connection, task, schema, domain, etc.) is not found. */
class StarlakeNotFoundException(message: String) extends StarlakeException(message)

/** Thrown when a required configuration value is missing or invalid. */
class StarlakeConfigException(message: String) extends StarlakeException(message)
