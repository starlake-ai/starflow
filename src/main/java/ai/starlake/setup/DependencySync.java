package ai.starlake.setup;

/**
 * Pure reconciliation between the artifacts the installer wants and the files already
 * on disk. No static state, no network, no writes: everything it needs is passed in.
 */
public final class DependencySync {

    private DependencySync() {
    }

    /**
     * Name fragment shared by every version of an artifact, used to spot superseded
     * copies on disk.
     *
     * <p>Maven and GitHub Releases URLs both put the version in the second-to-last path
     * segment ({@code .../bundle/2.29.52/bundle-2.29.52.jar}, {@code .../download/v7.0.0/...}),
     * and the file name always contains it, so truncating the name at that version is
     * exact - unlike guessing at the first digit, which turns
     * {@code spark-4.1-bigquery-0.44.2-preview.jar} into a dangerously broad {@code spark-}.
     *
     * <p>When the URL carries no version segment (the python wheels are served from a flat
     * directory), fall back to the name up to the first {@code -} followed by a digit, and
     * finally to the whole name.
     */
    public static String derivePrefix(String url, String fileName) {
        String versionSegment = versionSegment(url);
        if (versionSegment != null && !versionSegment.isEmpty()) {
            int at = fileName.indexOf(versionSegment);
            if (at > 0) {
                return fileName.substring(0, at);
            }
        }
        for (int i = 1; i < fileName.length() - 1; i++) {
            if (fileName.charAt(i) == '-' && Character.isDigit(fileName.charAt(i + 1))) {
                return fileName.substring(0, i + 1);
            }
        }
        return fileName;
    }

    /** Second-to-last path segment of the url, with any leading {@code v} stripped. */
    private static String versionSegment(String url) {
        String path = url;
        int query = path.indexOf('?');
        if (query >= 0) {
            path = path.substring(0, query);
        }
        int last = path.lastIndexOf('/');
        if (last <= 0) {
            return null;
        }
        int previous = path.lastIndexOf('/', last - 1);
        if (previous < 0) {
            return null;
        }
        String segment = path.substring(previous + 1, last);
        if (segment.length() > 1 && segment.charAt(0) == 'v' && Character.isDigit(segment.charAt(1))) {
            segment = segment.substring(1);
        }
        return segment;
    }
}
