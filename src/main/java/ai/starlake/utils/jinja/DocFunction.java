package ai.starlake.utils.jinja;

/**
 * Jinja function used while YAML files are loaded. It returns its own call text so that the
 * reference survives load-time rendering and is resolved later by the site generator.
 */
public final class DocFunction {
  private DocFunction() {}

  public static String doc(String name) {
    return "{{ doc(\"" + name + "\") }}";
  }
}
