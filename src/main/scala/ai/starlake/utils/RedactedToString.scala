package ai.starlake.utils

/** The `toString` of a case class holding secrets: these classes end up in logs and error messages.
  * Its fields print as usual, except those named like a credential, which only tell whether they
  * are set, and the credentials among the options of a map.
  */
trait RedactedToString extends Product {
  override def toString: String =
    productElementNames
      .zip(productIterator)
      .map {
        case (name, None)                      => s"$name=None"
        case (name, _) if Utils.isSecret(name) => s"$name=********"
        case (name, options: Map[_, _]) =>
          val printable = options.map { case (key, value) => key.toString -> String.valueOf(value) }
          s"$name=${Utils.redact(printable)}"
        case (name, value) => s"$name=$value"
      }
      .mkString(s"$productPrefix(", ", ", ")")
}
