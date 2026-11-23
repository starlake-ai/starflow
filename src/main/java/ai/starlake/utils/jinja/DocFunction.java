package ai.starlake.utils.jinja;

/**
 * Jinja function used while YAML files are loaded. It emits the quote-free marker {@code
 * <!--sl-doc:NAME-->}, which is valid inside any YAML quoting style. SchemaHandler replaces it with
 * the block's Markdown once the YAML is parsed, so warehouse descriptions and the site both get the
 * text.
 */
public final class DocFunction {
  private DocFunction() {}

  public static String doc(String name) {
    return "<!--sl-doc:" + name + "-->";
  }
}
