package ai.starlake.setup;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /**
     * Diff the artifacts the installer wants against what {@code dir} already holds.
     *
     * @param artifacts   every artifact the installer knows about, ENABLED AND DISABLED
     *                    alike - the disabled ones are what let a turned-off category get
     *                    cleaned up instead of orphaned
     * @param dir         directory to reconcile; a directory that does not exist is treated
     *                    as empty
     * @param remoteSizes remote byte size per {@link Artifact#url}; a missing entry or a
     *                    value {@code <= 0} means the size could not be determined, and a
     *                    correctly named file is then kept (so a fully provisioned install
     *                    is an offline no-op instead of a hard failure)
     * @param force       classify every enabled artifact as a download, whatever is on disk
     */
    public static SyncPlan reconcile(List<Artifact> artifacts, File dir, Map<String, Long> remoteSizes, boolean force) {
        SyncPlan plan = new SyncPlan();

        Set<String> desiredNames = new HashSet<>();
        for (Artifact artifact : artifacts) {
            if (artifact.enabled) {
                desiredNames.add(artifact.fileName);
            }
        }

        File[] present = dir.listFiles();
        if (present == null) {
            present = new File[0];
        }

        for (Artifact artifact : artifacts) {
            if (!artifact.enabled) {
                continue;
            }
            File target = new File(dir, artifact.fileName);
            long remote = sizeOf(remoteSizes, artifact.url);
            boolean usable = !force
                    && target.isFile()
                    && (remote <= 0 || target.length() == remote);
            if (usable) {
                plan.addUpToDate(target);
            } else {
                plan.add(new SyncPlan.Download(artifact, target, remote));
            }
        }

        for (File file : present) {
            if (!file.isFile() || desiredNames.contains(file.getName())) {
                continue;
            }
            Artifact owner = ownerOf(file.getName(), artifacts);
            if (owner != null) {
                String reason = owner.enabled ? "superseded" : owner.label + " disabled";
                plan.add(new SyncPlan.Deletion(file, reason));
            }
        }
        return plan;
    }

    /**
     * The artifact a file on disk belongs to, or null when nothing manages it - a jar the
     * user hand-copied into bin/deps, which the installer must never touch.
     *
     * <p>Matched against the file NAME. The old code matched {@code File.getPath()}, so an
     * installation directory whose path happened to contain an artefact name made every
     * file in bin/deps a match and got it deleted.
     */
    static Artifact ownerOf(String fileName, List<Artifact> artifacts) {
        for (Artifact artifact : artifacts) {
            for (String prefix : artifact.ownershipPrefixes) {
                if (!prefix.isEmpty() && fileName.contains(prefix)) {
                    return artifact;
                }
            }
        }
        return null;
    }

    private static long sizeOf(Map<String, Long> remoteSizes, String url) {
        Long size = remoteSizes.get(url);
        return size == null ? -1L : size;
    }
}
