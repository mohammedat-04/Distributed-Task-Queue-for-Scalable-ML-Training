package de.luh.vss.chat.dtq.master;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.dtq.job.JobStatus;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Persists master job history to disk.
 */
final class MasterStateStore {
    private static final String KEY_JOB_IDS = "job.ids";
    private final Path path;

    MasterStateStore(Path path) {
        this.path = path;
    }

    static final class JobSnapshot {
        final String jobId;
        final int producerId;
        final JobStatus status;
        final Message.JobType jobType;
        final String payload;
        final String result;
        final long updatedAt;

        JobSnapshot(String jobId,
                    int producerId,
                    JobStatus status,
                    Message.JobType jobType,
                    String payload,
                    String result,
                    long updatedAt) {
            this.jobId = jobId;
            this.producerId = producerId;
            this.status = status;
            this.jobType = jobType;
            this.payload = payload;
            this.result = result;
            this.updatedAt = updatedAt;
        }
    }

    Map<String, JobSnapshot> load() {
        Properties props = new Properties();
        Map<String, JobSnapshot> result = new LinkedHashMap<>();
        if (!Files.exists(path)) {
            return result;
        }
        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            props.load(in);
        } catch (IOException e) {
            System.err.println("Failed to load master state: " + e.getMessage());
            return result;
        }
        List<String> ids = splitIds(props.getProperty(KEY_JOB_IDS, ""));
        for (String jobId : ids) {
            String prefix = "job." + jobId + ".";
            String statusRaw = props.getProperty(prefix + "status");
            String typeRaw = props.getProperty(prefix + "type");
            if (statusRaw == null || typeRaw == null) {
                continue;
            }
            JobStatus status;
            Message.JobType jobType;
            try {
                status = JobStatus.valueOf(statusRaw);
                jobType = Message.JobType.valueOf(typeRaw);
            } catch (IllegalArgumentException e) {
                continue;
            }
            int producerId = parseInt(props.getProperty(prefix + "producer"), 0);
            String payload = props.getProperty(prefix + "payload", "");
            String resultPayload = props.getProperty(prefix + "result", "");
            long updatedAt = parseLong(props.getProperty(prefix + "updatedAt"), 0L);
            result.put(jobId, new JobSnapshot(jobId, producerId, status, jobType, payload, resultPayload, updatedAt));
        }
        return result;
    }

    synchronized void save(Map<String, JobSnapshot> jobs) {
        Properties props = new Properties();
        props.setProperty(KEY_JOB_IDS, String.join(",", jobs.keySet()));
        for (JobSnapshot job : jobs.values()) {
            String prefix = "job." + job.jobId + ".";
            props.setProperty(prefix + "producer", Integer.toString(job.producerId));
            props.setProperty(prefix + "status", job.status.name());
            props.setProperty(prefix + "type", job.jobType.name());
            if (job.payload != null) {
                props.setProperty(prefix + "payload", job.payload);
            }
            if (job.result != null) {
                props.setProperty(prefix + "result", job.result);
            }
            props.setProperty(prefix + "updatedAt", Long.toString(job.updatedAt));
        }
        writeProperties(props);
    }

    private void writeProperties(Properties props) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tmp))) {
                props.store(out, "master-state");
            }
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Failed to persist master state: " + e.getMessage());
        }
    }

    private static List<String> splitIds(String raw) {
        List<String> ids = new ArrayList<>();
        if (raw == null || raw.trim().isEmpty()) {
            return ids;
        }
        String[] parts = raw.split(",");
        for (String part : parts) {
            String id = part.trim();
            if (!id.isEmpty()) {
                ids.add(id);
            }
        }
        return ids;
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long parseLong(String value, long fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
