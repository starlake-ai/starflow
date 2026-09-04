package ai.starlake.setup;

import java.util.Collections;
import java.util.List;

/**
 * One artifact the installer manages, described as plain data so the reconciler never
 * needs to know about Setup's ResourceDependency.
 *
 * <p>A disabled artifact still carries its ownership prefixes: that is what lets the
 * reconciler remove the jars of a category the user has just turned off.
 */
public final class Artifact {

    /** Category label used in the printed plan, e.g. "Snowflake". */
    public final String label;

    /** File name this artifact must have on disk once installed. */
    public final String fileName;

    /** URL the artifact is downloaded from. */
    public final String url;

    /**
     * Name fragments that identify a file on disk as belonging to this artifact,
     * whatever its version. Matched with {@link String#contains(CharSequence)} against
     * the file NAME - never the path.
     */
    public final List<String> ownershipPrefixes;

    /** Whether this artifact is wanted for the current run. */
    public final boolean enabled;

    public Artifact(String label, String fileName, String url, List<String> ownershipPrefixes, boolean enabled) {
        this.label = label;
        this.fileName = fileName;
        this.url = url;
        this.ownershipPrefixes = Collections.unmodifiableList(ownershipPrefixes);
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return fileName + (enabled ? "" : " (disabled)");
    }
}
