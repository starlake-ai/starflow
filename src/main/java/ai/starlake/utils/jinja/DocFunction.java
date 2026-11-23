package ai.starlake.utils.jinja;

/**
 * Jinja function used while YAML files are loaded. It emits the quote-free marker {@code
 * <!--sl-doc:NAME-->}, which is valid inside any YAML quoting style and is resolved later by the
 * site generator.
 */
public final class DocFunction {
  private DocFunction() {}

  public static String doc(String name) {
    return "<!--sl-doc:" + name + "-->";
  }
}
