package de.luh.vss.chat.dtq.producer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Persists producer job history to disk.
 */
final class ProducerStateStore {
    private static final String KEY_JOB_IDS = "job.ids";
    private final Path path;

    ProducerStateStore(int producerId) {
        this.path = Paths.get("state", "producer_" + producerId + ".properties");
    }

    static final class State {
        final Map<String, String> jobStates = new HashMap<>();
        final Map<String, String> jobResults = new HashMap<>();
        final Map<String, String> jobPayloads = new HashMap<>();
        final Map<String, Long> jobSubmitTimes = new HashMap<>();
        final Map<String, Long> jobDurations = new HashMap<>();
    }

    State load() {
        State state = new State();
        if (!Files.exists(path)) {
            return state;
        }
        Properties props = new Properties();
        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            props.load(in);
        } catch (IOException e) {
            System.err.println("Failed to load producer state: " + e.getMessage());
            return state;
        }
        List<String> ids = splitIds(props.getProperty(KEY_JOB_IDS, ""));
        for (String jobId : ids) {
            String prefix = "job." + jobId + ".";
            String status = props.getProperty(prefix + "state");
            if (status != null) {
                state.jobStates.put(jobId, status);
            }
            String result = props.getProperty(prefix + "result");
            if (result != null) {
                state.jobResults.put(jobId, result);
            }
            String payload = props.getProperty(prefix + "payload");
            if (payload != null) {
                state.jobPayloads.put(jobId, payload);
            }
            long submit = parseLong(props.getProperty(prefix + "submit"), 0L);
            if (submit > 0) {
                state.jobSubmitTimes.put(jobId, submit);
            }
            long duration = parseLong(props.getProperty(prefix + "duration"), 0L);
            if (duration > 0) {
                state.jobDurations.put(jobId, duration);
            }
        }
        return state;
    }

    synchronized void save(Map<String, String> jobStates,
                           Map<String, String> jobResults,
                           Map<String, String> jobPayloads,
                           Map<String, Long> jobSubmitTimes,
                           Map<String, Long> jobDurations) {
        Properties props = new Properties();
        props.setProperty(KEY_JOB_IDS, String.join(",", jobStates.keySet()));
        for (String jobId : jobStates.keySet()) {
            String prefix = "job." + jobId + ".";
            String status = jobStates.get(jobId);
            if (status != null) {
                props.setProperty(prefix + "state", status);
            }
            String result = jobResults.get(jobId);
            if (result != null) {
                props.setProperty(prefix + "result", result);
            }
            String payload = jobPayloads.get(jobId);
            if (payload != null) {
                props.setProperty(prefix + "payload", payload);
            }
            Long submit = jobSubmitTimes.get(jobId);
            if (submit != null) {
                props.setProperty(prefix + "submit", Long.toString(submit));
            }
            Long duration = jobDurations.get(jobId);
            if (duration != null) {
                props.setProperty(prefix + "duration", Long.toString(duration));
            }
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
                props.store(out, "producer-state");
            }
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Failed to persist producer state: " + e.getMessage());
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
