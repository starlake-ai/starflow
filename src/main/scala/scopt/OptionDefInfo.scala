package scopt

/** What scopt keeps private about an option, for the CLI pages: whether it is a positional
  * argument, and whether it takes no value (a flag, as opt[Unit])
  */
object OptionDefInfo {
  def isArgument(opt: OptionDef[_, _]): Boolean = opt.kind == OptionDefKind.Arg

  def isFlag(opt: OptionDef[_, _]): Boolean = opt.read.arity == 0
}
