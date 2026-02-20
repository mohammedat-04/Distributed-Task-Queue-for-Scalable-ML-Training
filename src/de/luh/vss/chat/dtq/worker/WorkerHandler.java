package de.luh.vss.chat.dtq.worker;

import de.luh.vss.chat.common.Message;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes messages from the master and dispatches local handling logic.
 */
public class WorkerHandler implements Runnable {
    private final BlockingQueue<Message> receivedMessage;
    private final AtomicInteger availableSlots;
    private final Connection connection;
    private volatile boolean tasksReady;
    private final long taskTimeoutMs;
    private volatile String cancelTaskId;
    private final WorkerStateStore stateStore;
    private long completedTasks;
    private long failedTasks;
    private String lastTaskId;
    private String lastError;
    /**
     * Snapshot of worker stats for display.
     */
    public static final class WorkerStats {
        public final long completed;
        public final long failed;
        public final String lastTaskId;
        public final String lastError;

        private WorkerStats(long completed, long failed, String lastTaskId, String lastError) {
            this.completed = completed;
            this.failed = failed;
            this.lastTaskId = lastTaskId;
            this.lastError = lastError;
        }
    }
    /**
     * Creates a WorkerHandler.
     */
    public WorkerHandler(BlockingQueue<Message> receivedMessage,
                         int slots,
                         Connection connection,
                         long taskTimeoutMs) {
        this.receivedMessage = receivedMessage;
        this.availableSlots = new AtomicInteger(slots);
        this.connection = connection;
        this.tasksReady = true;
        this.taskTimeoutMs = taskTimeoutMs;
        this.cancelTaskId = null;
        this.stateStore = new WorkerStateStore(connection.getId());
        WorkerStateStore.State loaded = stateStore.load();
        this.completedTasks = loaded.completed;
        this.failedTasks = loaded.failed;
        this.lastTaskId = loaded.lastTaskId;
        this.lastError = loaded.lastError;
        if (completedTasks > 0 || failedTasks > 0) {
            System.out.println("Loaded worker state: completed=" + completedTasks + " failed=" + failedTasks);
        }
    }

    /**
     * Sends a task request using the current available slots.
     */
    private void sendTaskRequest() {
        if (!tasksReady) {
            return;
        }
        if (availableSlots.get() <= 0) {
            return;
        }
        availableSlots.decrementAndGet();
        try {
            Message.WorkerRequestTask req = new Message.WorkerRequestTask(connection.getId(), 1);
            connection.writeMessage(req);
            System.out.println("Sent WorkerRequestTask");
        } catch (IOException e) {
            availableSlots.incrementAndGet();
            System.err.println("Failed to send WorkerRequestTask");
        }
    }

    /**
     * Processes the next message from the master.
     */
    private void handleMessage(Message message) throws InterruptedException {

        if (message instanceof Message.ServiceErrorResponse errorResponse) {
            System.err.println("Received error from server: " + errorResponse.getErrorMessage());
            System.exit(1);
        } else if (message instanceof Message.ChatMessagePayload chatMessage) {
            System.out.println("Chat from " + chatMessage.getrole() + " " + chatMessage.getId() + ": " + chatMessage.getMessage());
            if (chatMessage.getMessage().startsWith("Tasks ready for job")) {
                tasksReady = true;
                sendTaskRequest();
            }
        } else if (message instanceof Message.MasterAssignTask task) {
            if(task.getJob() == Message.JobType.SUM_JOB){
                try {
                    String[] payload = task.getPayload().split("\\|", 2);
                    BigInteger start = new BigInteger(payload[0]);
                    BigInteger end = new BigInteger(payload[1]);
                    BigInteger count = end.subtract(start).add(BigInteger.ONE);
                    BigInteger sum = start
                            .add(end)
                            .multiply(count)
                            .divide(BigInteger.valueOf(2));
                    sendTaskResult(task.getTaskId(), sum.toString());
                    recordSuccess(task.getTaskId());
                } catch (RuntimeException e) {
                    sendTaskResult(task.getTaskId(), "FAILED");
                    recordFailure(task.getTaskId(), e.getMessage());
                    System.err.println("Failed to process task payload: " + task.getPayload());
                }
            } else if (task.getJob() == Message.JobType.SUMSQ_JOB) {
                try {
                    String[] payload = task.getPayload().split("\\|", 2);
                    BigInteger start = new BigInteger(payload[0]);
                    BigInteger end = new BigInteger(payload[1]);
                    BigInteger sumSq = sumSquares(end).subtract(sumSquares(start.subtract(BigInteger.ONE)));
                    sendTaskResult(task.getTaskId(), sumSq.toString());
                    recordSuccess(task.getTaskId());
                } catch (RuntimeException e) {
                    sendTaskResult(task.getTaskId(), "FAILED");
                    recordFailure(task.getTaskId(), e.getMessage());
                    System.err.println("Failed to process task payload: " + task.getPayload());
                }
            } else if (task.getJob() == Message.JobType.GRADIEN_JOB) {
                try {
                    TaskPayload payload = parseTrainTaskPayload(task.getPayload());
                    if (task.getTaskId().equals(cancelTaskId)) {
                        sendTaskResult(task.getTaskId(), "FAILED|reason=CANCELLED");
                        cancelTaskId = null;
                        return;
                    }
                    TrainGdBatchWorker.ProgressListener listener =
                            (processed, total) -> {
                                try {
                                    connection.writeMessage(new Message.WorkerTaskProgress(
                                            connection.getId(), task.getTaskId(), processed, total));
                                } catch (IOException e) {
                                    // best-effort; ignore
                                }
                            };
                    TaskResult result = taskTimeoutMs > 0
                            ? TrainGdBatchWorker.computeGradient(payload, taskTimeoutMs, listener)
                            : TrainGdBatchWorker.computeGradient(payload, 0L, listener);
                    String resultPayload = buildGradientResult(result);
                    sendTaskResult(task.getTaskId(), resultPayload);
                    recordSuccess(task.getTaskId());
                } catch (RuntimeException e) {
                    int attempt = parseAttemptFromPayload(task.getPayload());
                    String errorMsg = e.getMessage();
                    if (errorMsg != null && errorMsg.contains("DATAREF_NOT_FOUND")) {
                        sendTaskResult(task.getTaskId(), "FAILED|reason=DATAREF");
                        recordFailure(task.getTaskId(), errorMsg);
                    } else if (errorMsg != null && errorMsg.contains("TASK_TIMEOUT")) {
                        sendTaskResult(task.getTaskId(), "FAILED|reason=TIMEOUT");
                        recordFailure(task.getTaskId(), errorMsg);
                    } else {
                        String failedPayload = attempt >= 0 ? "FAILED|attempt=" + attempt : "FAILED";
                        sendTaskResult(task.getTaskId(), failedPayload);
                        recordFailure(task.getTaskId(), errorMsg);
                    }
                    System.err.println("Failed to process task payload: " + task.getPayload());
                }
            }
            System.out.println("Received task: " + ((Message.MasterAssignTask) message).getTaskId());
            availableSlots.incrementAndGet();
            sendTaskRequest();
        } else if (message instanceof Message.MasterCancelTask cancel) {
            cancelTaskId = cancel.getTaskId();
        } else if (message instanceof Message.MasterNoTask) {
            availableSlots.incrementAndGet();
            tasksReady = false;
            Thread.sleep(500);
        } else {
            System.out.println("Received unknown message: " + message);
        }
    }

    /**
     * Runs the main receive loop for handling incoming messages.
     */
    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Message message = receivedMessage.poll(500, TimeUnit.MILLISECONDS);
                if (message != null) {
                    handleMessage(message);
                }
                sendTaskRequest();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sends task Result.
     */
    private void sendTaskResult(String taskId, String payload) {
        try {
            Message.WorkerTaskResult result = new Message.WorkerTaskResult(connection.getId(), taskId, payload);
            connection.writeMessage(result);
            System.out.println("Sent WorkerTaskResult for task " + taskId);
        } catch (IOException e) {
            System.err.println("Failed to send WorkerTaskResult");
        }
    }

    /**
     * Returns a snapshot of local worker stats.
     */
    public WorkerStats snapshotStats() {
        return new WorkerStats(completedTasks, failedTasks, lastTaskId, lastError);
    }

    private void recordSuccess(String taskId) {
        completedTasks++;
        lastTaskId = taskId;
        persistState(null);
    }

    private void recordFailure(String taskId, String errorMsg) {
        failedTasks++;
        lastTaskId = taskId;
        lastError = errorMsg;
        persistState(errorMsg);
    }

    private void persistState(String errorMsg) {
        stateStore.save(new WorkerStateStore.State(
                completedTasks,
                failedTasks,
                lastTaskId,
                errorMsg != null ? errorMsg : lastError,
                System.currentTimeMillis()));
    }

    /**
     * Parses train Task Payload.
     */
    private TaskPayload parseTrainTaskPayload(String payload) {
        if (payload == null || payload.trim().isEmpty()) {
            throw new IllegalArgumentException("payload is empty");
        }
        Map<String, String> fields = new HashMap<>();
        String[] parts = payload.split("\\|");
        for (String part : parts) {
            String token = part.trim();
            if (token.isEmpty()) {
                continue;
            }
            String[] kv = token.split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            fields.put(kv[0].trim(), kv[1].trim());
        }
        String jobId = requireField(fields, "jobId");
        int epoch = parseIntField(fields, "epoch");
        int batchId = parseIntField(fields, "batchId");
        int start = parseIntField(fields, "start");
        int count = parseIntField(fields, "count");
        int features = parseIntField(fields, "features");
        String model = requireField(fields, "model");
        String task = fields.getOrDefault("task", "");
        String dataRef = requireField(fields, "dataRef");
        int attempt = parseIntField(fields, "attempt", 0);
        if (start < 0 || count < 0) {
            throw new IllegalArgumentException("start/count must be >= 0");
        }
        if (features <= 0) {
            throw new IllegalArgumentException("features must be > 0");
        }
        double[] w = parseWeights(fields.get("w"), features);
        boolean hasHeader = Boolean.parseBoolean(fields.getOrDefault("hasHeader", "false"));
        String csvLabel = fields.getOrDefault("csvLabel", "last");
        double normalize = parseDoubleField(fields, "normalize", 1.0);
        return new TaskPayload(jobId, epoch, batchId, start, count, features, model, task, dataRef, w,
                hasHeader, csvLabel, normalize, attempt);
    }

    /**
     * Builds gradient Result.
     */
    private String buildGradientResult(TaskResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append(result.getCount());
        sb.append('|').append(result.getLossSum());
        sb.append("|gradSum=");
        double[] grad = result.getGradSum();
        if (grad != null && grad.length > 0) {
            for (int i = 0; i < grad.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(grad[i]);
            }
        }
        sb.append("|attempt=").append(result.getAttempt());
        return sb.toString();
    }

    /**
     * Performs require Field.
     */
    private String requireField(Map<String, String> fields, String key) {
        String value = fields.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing field: " + key);
        }
        return value;
    }

    /**
     * Parses int Field.
     */
    private int parseIntField(Map<String, String> fields, String key) {
        String value = requireField(fields, key);
        return Integer.parseInt(value);
    }

    /**
     * Parses int Field with fallback.
     */
    private int parseIntField(Map<String, String> fields, String key, int fallback) {
        String value = fields.get(key);
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Parses attempt from payload.
     */
    private int parseAttemptFromPayload(String payload) {
        if (payload == null || payload.isEmpty()) {
            return -1;
        }
        String[] parts = payload.split("\\|");
        for (String part : parts) {
            String token = part.trim();
            if (token.startsWith("attempt=")) {
                String value = token.substring("attempt=".length()).trim();
                try {
                    return Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
        }
        return -1;
    }

    /**
     * Parses double Field.
     */
    private double parseDoubleField(Map<String, String> fields, String key, double fallback) {
        String value = fields.get(key);
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Parses weights.
     */
    private double[] parseWeights(String raw, int features) {
        double[] weights = new double[features];
        if (raw == null || raw.trim().isEmpty()) {
            return weights;
        }
        String[] parts = raw.split(",");
        int limit = Math.min(parts.length, features);
        for (int i = 0; i < limit; i++) {
            weights[i] = Double.parseDouble(parts[i].trim());
        }
        return weights;
    }

    /**
     * Sum of squares from 1..n using formula n(n+1)(2n+1)/6.
     */
    private BigInteger sumSquares(BigInteger n) {
        if (n.compareTo(BigInteger.ZERO) <= 0) {
            return BigInteger.ZERO;
        }
        BigInteger twoNPlusOne = n.multiply(BigInteger.valueOf(2)).add(BigInteger.ONE);
        return n.multiply(n.add(BigInteger.ONE)).multiply(twoNPlusOne).divide(BigInteger.valueOf(6));
    }

  
}
