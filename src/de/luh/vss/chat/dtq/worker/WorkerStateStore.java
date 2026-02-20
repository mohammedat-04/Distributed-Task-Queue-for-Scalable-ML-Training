package de.luh.vss.chat.dtq.worker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * Persists worker stats to disk.
 */
final class WorkerStateStore {
    private final Path path;

    WorkerStateStore(int workerId) {
        this.path = Paths.get("state", "worker_" + workerId + ".properties");
    }

    static final class State {
        final long completed;
        final long failed;
        final String lastTaskId;
        final String lastError;
        final long updatedAt;

        State(long completed, long failed, String lastTaskId, String lastError, long updatedAt) {
            this.completed = completed;
            this.failed = failed;
            this.lastTaskId = lastTaskId;
            this.lastError = lastError;
            this.updatedAt = updatedAt;
        }
    }

    State load() {
        if (!Files.exists(path)) {
            return new State(0L, 0L, null, null, 0L);
        }
        Properties props = new Properties();
        try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            props.load(in);
        } catch (IOException e) {
            System.err.println("Failed to load worker state: " + e.getMessage());
            return new State(0L, 0L, null, null, 0L);
        }
        long completed = parseLong(props.getProperty("completed"), 0L);
        long failed = parseLong(props.getProperty("failed"), 0L);
        String lastTask = props.getProperty("lastTaskId");
        String lastError = props.getProperty("lastError");
        long updatedAt = parseLong(props.getProperty("updatedAt"), 0L);
        return new State(completed, failed, lastTask, lastError, updatedAt);
    }

    synchronized void save(State state) {
        Properties props = new Properties();
        props.setProperty("completed", Long.toString(state.completed));
        props.setProperty("failed", Long.toString(state.failed));
        if (state.lastTaskId != null) {
            props.setProperty("lastTaskId", state.lastTaskId);
        }
        if (state.lastError != null) {
            props.setProperty("lastError", state.lastError);
        }
        props.setProperty("updatedAt", Long.toString(state.updatedAt));
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
                props.store(out, "worker-state");
            }
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Failed to persist worker state: " + e.getMessage());
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
