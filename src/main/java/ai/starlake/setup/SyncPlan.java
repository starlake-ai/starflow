package ai.starlake.setup;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The difference between the artifacts the installer wants and what is on disk. */
public final class SyncPlan {

    /** An artifact that must be fetched, with the remote size when it could be determined. */
    public static final class Download {
        public final Artifact artifact;
        public final File target;
        /** Remote byte size, or -1 when it could not be determined. */
        public final long size;

        public Download(Artifact artifact, File target, long size) {
            this.artifact = artifact;
            this.target = target;
            this.size = size;
        }
    }

    /** A file that must go, with the reason shown to the user. */
    public static final class Deletion {
        public final File file;
        public final String reason;

        public Deletion(File file, String reason) {
            this.file = file;
            this.reason = reason;
        }
    }

    private final List<Download> toDownload = new ArrayList<>();
    private final List<Deletion> toDelete = new ArrayList<>();
    private final List<File> upToDate = new ArrayList<>();

    public void add(Download download) {
        toDownload.add(download);
    }

    public void add(Deletion deletion) {
        toDelete.add(deletion);
    }

    public void addUpToDate(File file) {
        upToDate.add(file);
    }

    public List<Download> getToDownload() {
        return Collections.unmodifiableList(toDownload);
    }

    public List<Deletion> getToDelete() {
        return Collections.unmodifiableList(toDelete);
    }

    public List<File> getUpToDate() {
        return Collections.unmodifiableList(upToDate);
    }

    public boolean isEmpty() {
        return toDownload.isEmpty() && toDelete.isEmpty();
    }

    /** Total of the KNOWN download sizes; artifacts of unknown size contribute nothing. */
    public long bytesToDownload() {
        long total = 0;
        for (Download download : toDownload) {
            if (download.size > 0) {
                total += download.size;
            }
        }
        return total;
    }

    public void mergeFrom(SyncPlan other) {
        toDownload.addAll(other.toDownload);
        toDelete.addAll(other.toDelete);
        upToDate.addAll(other.upToDate);
    }

    /**
     * Human-readable plan. The no-op path - by far the most common one once this feature
     * lands - is deliberately a single line: a fast upgrade should look fast, not silent.
     */
    public String render(String header) {
        if (isEmpty()) {
            return "All " + upToDate.size() + " dependencies up to date, nothing to download.";
        }
        StringBuilder sb = new StringBuilder(header).append("\n");
        sb.append("  = ").append(upToDate.size()).append(" up to date\n");
        if (!toDownload.isEmpty()) {
            long known = bytesToDownload();
            // "0 B" would read as "nothing to transfer" when in fact no size could be
            // determined - which is the normal case under SL_FORCE_DOWNLOAD, where probing is
            // skipped entirely.
            sb.append("  + ").append(toDownload.size()).append(" to download (")
              .append(known > 0 ? humanSize(known) : "size unknown").append(")\n");
            for (Download download : toDownload) {
                sb.append("      ").append(download.artifact.fileName).append("  ")
                  .append(download.size > 0 ? "(" + humanSize(download.size) + ")" : "(unknown size)")
                  .append("\n");
            }
        }
        if (!toDelete.isEmpty()) {
            sb.append("  - ").append(toDelete.size()).append(" to remove\n");
            for (Deletion deletion : toDelete) {
                sb.append("      ").append(deletion.file.getName()).append("  (")
                  .append(deletion.reason).append(")\n");
            }
        }
        return sb.toString();
    }

    private static String humanSize(long bytes) {
        if (bytes >= 1024L * 1024L) {
            return (bytes / 1024L / 1024L) + " MB";
        }
        if (bytes >= 1024L) {
            return (bytes / 1024L) + " KB";
        }
        return bytes + " B";
    }
}
