package ai.starlake.job.metrics

/** The failure of a task whose write was rolled back because an expectation that it checked before
  * its commit failed (#1976)
  */
class ExpectationsFailedBeforeCommitException(message: String) extends Exception(message)
