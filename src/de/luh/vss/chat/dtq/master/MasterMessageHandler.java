package de.luh.vss.chat.dtq.master;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.common.SystemRole;
import de.luh.vss.chat.dtq.job.Job;
import de.luh.vss.chat.dtq.job.JobStatus;
import de.luh.vss.chat.dtq.job.TrainGdBatchJob;
import de.luh.vss.chat.dtq.task.TaskSeq;
import de.luh.vss.chat.dtq.task.TaskState;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.file.Paths;

/**
 * Tracks task bookkeeping and aggregated results for a single job.
 */
class JobState{
    private ConcurrentHashMap<String,TaskSeq> tasks;
    private AtomicInteger taskNumber;
    private Job job;
    private final Set<Integer> workerIds = ConcurrentHashMap.newKeySet();
    /**
     * Creates a JobState.
     */
    public JobState( ConcurrentHashMap<String,TaskSeq> task,AtomicInteger taskNumber,Job job){
        this.tasks = task;
        this.taskNumber = taskNumber;
        this.job = job;
    }
    /**
     * Performs decrement.
     */
    public int decrement(){return taskNumber.decrementAndGet();}
    /**
     * Sets task State.
     */
    public void setTaskState(TaskState state,String taskId){
        TaskSeq task = tasks.get(taskId);
        task.setState(state);
    }
    /**
     * Sets task Result.
     */
    public void setTaskResult(String taskId, String result) {
        TaskSeq task = tasks.get(taskId);
        if (task != null) {
            task.setResult(result);
        }
    }
    /**
     * Sets result.
     */
    public void setResult(String result){
        this.job.setResult(result);
    }
    /**
     * Returns task.
     */
    public TaskSeq getTask(String taskId) {
        return tasks.get(taskId);
    }
    /**
     * Returns job.
     */
    public Job getJob() {
        return job;
    }
    /**
     * Returns tasks.
     */
    public Iterable<TaskSeq> getTasks() {
        return tasks.values();
    }
    /**
     * Resets tasks.
     */
    public void resetTasks(ConcurrentHashMap<String, TaskSeq> tasks, int taskCount) {
        this.tasks = tasks;
        this.taskNumber.set(taskCount);
    }
    /**
     * Records worker participation for metrics.
     */
    public void noteWorker(int workerId) {
        if (workerId > 0) {
            workerIds.add(workerId);
        }
    }
    /**
     * Returns worker count.
     */
    public int workerCount() {
        return workerIds.size();
    }
    /**
     * Builds result Payload.
     */
    public String buildResultPayload() {
        if (job.getJobType() == Message.JobType.SUM_JOB) {
            BigInteger total = BigInteger.ZERO;
            for (TaskSeq task : tasks.values()) {
                String res = task.getResult();
                if (res != null && !res.isEmpty()) {
                    total = total.add(new BigInteger(res));
                }
            }
            return total.toString();
        }
        if (job.getJobType() == Message.JobType.SUMSQ_JOB) {
            BigInteger total = BigInteger.ZERO;
            for (TaskSeq task : tasks.values()) {
                String res = task.getResult();
                if (res != null && !res.isEmpty()) {
                    total = total.add(new BigInteger(res));
                }
            }
            return total.toString();
        }
        if (job.getJobType() == Message.JobType.GRADIEN_JOB && job instanceof TrainGdBatchJob trainJob) {
            return trainJob.buildResultPayload();
        }
        return null;
    }
}


/**
 * Coordinates master-side scheduling, task assignment, and result aggregation.
 * Consumes inbound messages from the master queue.
 */
public class MasterMessageHandler implements Runnable {

    private final BlockingQueue<Message> messageQueue;
    private final ConcurrentHashMap<Integer, WorkerInfo> workers ;
    private final ConcurrentHashMap<Integer, ProducerInfo> producers ; 
    private final ConcurrentHashMap<String ,JobState> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> jobResults = new ConcurrentHashMap<>();
    private final BlockingQueue<Message> toSendMessages;
    private  BlockingQueue<TaskSeq> taskQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Long> jobStartMs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> epochStartMs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, WorkerPerf> workerPerf = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, TaskAssignment>> taskAssignments = new ConcurrentHashMap<>();
    private final Set<String> speculativeTasks = ConcurrentHashMap.newKeySet();
    private final MasterStateStore stateStore = new MasterStateStore(Paths.get("state/master_state.properties"));

    private static  int jobCounter = 0;
    private static final long TASK_LEASE_MS = 60_000L;
    private static final long TASK_SPECULATE_MS = 20_000L;
    private static final long TASK_LONG_TIMEOUT_MS = 180_000L;
    private static final long TASK_PROGRESS_TIMEOUT_MS = 30_000L;
    private static final int MAX_TASK_ATTEMPTS = 5;
    private static final long BACKOFF_BASE_MS = 2_000L;
    private static final long BACKOFF_MAX_MS = 30_000L;
    private volatile long lastReclaimCheck = 0L;

    /**
     * Tracks worker performance stats.
     */
    private static final class WorkerPerf {
        private long assigned;
        private long completed;
        private long failed;
        private long inFlight;
        private long totalMs;
        private long lastUpdate;

        synchronized void onAssigned(long now) {
            assigned++;
            inFlight++;
            lastUpdate = now;
        }

        synchronized void onCompleted(long now, long durationMs) {
            completed++;
            if (durationMs > 0) {
                totalMs += durationMs;
            }
            if (inFlight > 0) {
                inFlight--;
            }
            lastUpdate = now;
        }

        synchronized void onFailed(long now, long durationMs) {
            failed++;
            if (inFlight > 0) {
                inFlight--;
            }
            lastUpdate = now;
        }

        synchronized void onLate(long now) {
            if (inFlight > 0) {
                inFlight--;
            }
            lastUpdate = now;
        }

        synchronized WorkerPerfSnapshot snapshot(int workerId) {
            long avgMs = completed > 0 ? totalMs / completed : 0L;
            return new WorkerPerfSnapshot(workerId, assigned, completed, failed, inFlight, avgMs, lastUpdate);
        }
    }

    /**
     * Task assignment tracking for performance timing.
     */
    private static final class TaskAssignment {
        private final int workerId;
        private final long startMs;
        private volatile long lastProgressMs;
        private volatile int processed;
        private volatile int total;

        private TaskAssignment(int workerId, long startMs) {
            this.workerId = workerId;
            this.startMs = startMs;
            this.lastProgressMs = startMs;
            this.processed = 0;
            this.total = 0;
        }
    }

    /**
     * Immutable snapshot of worker performance.
     */
    public static final class WorkerPerfSnapshot {
        public final int workerId;
        public final long assigned;
        public final long completed;
        public final long failed;
        public final long inFlight;
        public final long avgMs;
        public final long lastUpdateMs;

        private WorkerPerfSnapshot(int workerId,
                                   long assigned,
                                   long completed,
                                   long failed,
                                   long inFlight,
                                   long avgMs,
                                   long lastUpdateMs) {
            this.workerId = workerId;
            this.assigned = assigned;
            this.completed = completed;
            this.failed = failed;
            this.inFlight = inFlight;
            this.avgMs = avgMs;
            this.lastUpdateMs = lastUpdateMs;
        }
    }

    /**
     * Parsed gradient payload returned by a worker.
     */
    private static class GradientResult {
        private final int count;
        private final double lossSum;
        private final double[] gradSum;

        /**
         * Creates a GradientResult.
         */
        private GradientResult(int count, double lossSum, double[] gradSum) {
            this.count = count;
            this.lossSum = lossSum;
            this.gradSum = gradSum;
        }
    }

    /**
     * Creates a MasterMessageHandler.
     */
    public MasterMessageHandler(BlockingQueue<Message> messageQueue,BlockingQueue<Message> toSendMessages ,ConcurrentHashMap<Integer,  WorkerInfo> workers, 
        ConcurrentHashMap<Integer, ProducerInfo> producers){
        this.workers = workers;
        this.toSendMessages = toSendMessages;
        this.producers = producers;
        this.messageQueue = messageQueue;
        
    }

    /**
     * Loads persisted completed job history into the in-memory snapshot.
     */
    public void loadPersistedJobs() {
        Map<String, MasterStateStore.JobSnapshot> persisted = stateStore.load();
        if (persisted.isEmpty()) {
            return;
        }
        for (MasterStateStore.JobSnapshot snap : persisted.values()) {
            Job job = new Job(snap.jobId, snap.producerId, snap.status, snap.jobType, snap.payload);
            JobState jobState = new JobState(new ConcurrentHashMap<>(), new AtomicInteger(0), job);
            jobs.putIfAbsent(snap.jobId, jobState);
            if (snap.result != null && !snap.result.isEmpty()) {
                jobResults.put(snap.jobId, snap.result);
            }
        }
    }

    /**
     * Persists completed/failed jobs to disk.
     */
    public void persistCompletedJobs() {
        Map<String, MasterStateStore.JobSnapshot> snapshot = new LinkedHashMap<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, JobState> entry : jobs.entrySet()) {
            Job job = entry.getValue().getJob();
            if (job == null) {
                continue;
            }
            if (job.getStatus() != JobStatus.COMPLETED && job.getStatus() != JobStatus.FAILD) {
                continue;
            }
            String result = jobResults.get(entry.getKey());
            snapshot.put(entry.getKey(), new MasterStateStore.JobSnapshot(
                    job.getJobId(),
                    job.getProducerId(),
                    job.getStatus(),
                    job.getJobType(),
                    job.getPayload(),
                    result,
                    now));
        }
        stateStore.save(snapshot);
    }

    /**
     * Builds a snapshot of job States.
     */
    public Map<String, JobStatus> snapshotJobStates() {
        Map<String, JobStatus> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, JobState> entry : jobs.entrySet()) {
            Job job = entry.getValue().getJob();
            snapshot.put(entry.getKey(), job != null ? job.getStatus() : null);
        }
        return snapshot;
    }

    /**
     * Returns job -> producerId mapping for display.
     */
    public Map<String, Integer> snapshotJobProducers() {
        Map<String, Integer> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, JobState> entry : jobs.entrySet()) {
            Job job = entry.getValue().getJob();
            if (job != null) {
                snapshot.put(entry.getKey(), job.getProducerId());
            }
        }
        return snapshot;
    }

    /**
     * Returns a snapshot of a worker's performance metrics.
     */
    public WorkerPerfSnapshot getWorkerPerfSnapshot(int workerId) {
        WorkerPerf perf = workerPerf.get(workerId);
        if (perf == null) {
            return new WorkerPerfSnapshot(workerId, 0, 0, 0, 0, 0, 0);
        }
        return perf.snapshot(workerId);
    }

    /**
     * Snapshot of job progress including task counts and batch info (if training).
     */
    public static final class JobProgress {
        public final String jobId;
        public final JobStatus status;
        public final int totalTasks;
        public final int remainingTasks;
        public final int epoch;
        public final int batchId;
        public final int batchesPerEpoch;

        private JobProgress(String jobId,
                            JobStatus status,
                            int totalTasks,
                            int remainingTasks,
                            int epoch,
                            int batchId,
                            int batchesPerEpoch) {
            this.jobId = jobId;
            this.status = status;
            this.totalTasks = totalTasks;
            this.remainingTasks = remainingTasks;
            this.epoch = epoch;
            this.batchId = batchId;
            this.batchesPerEpoch = batchesPerEpoch;
        }
    }

    /**
     * Returns job progress snapshots for all known jobs.
     */
    public List<JobProgress> snapshotJobProgress() {
        List<JobProgress> progress = new ArrayList<>();
        for (Map.Entry<String, JobState> entry : jobs.entrySet()) {
            JobState jobState = entry.getValue();
            Job job = jobState.getJob();
            int total = 0;
            int remaining = 0;
            for (TaskSeq task : jobState.getTasks()) {
                total++;
                TaskState state = task.getState();
                if (state == TaskState.PENDING || state == TaskState.RUNNING) {
                    remaining++;
                }
            }
            if (job != null && job.getStatus() == JobStatus.FAILD) {
                remaining = 0;
            }
            int epoch = -1;
            int batchId = -1;
            int batchesPerEpoch = -1;
            if (job instanceof TrainGdBatchJob trainJob) {
                epoch = trainJob.getCurrentEpoch();
                batchId = trainJob.getCurrentBatchId();
                int samples = trainJob.getSamples();
                int batchSize = trainJob.getBatchSize();
                if (samples > 0 && batchSize > 0) {
                    batchesPerEpoch = (samples + batchSize - 1) / batchSize;
                }
            }
            progress.add(new JobProgress(entry.getKey(),
                    job != null ? job.getStatus() : null,
                    total,
                    remaining,
                    epoch,
                    batchId,
                    batchesPerEpoch));
        }
        return progress;
    }

    /**
     * Snapshot of inflight tasks for verbose worker output.
     */
    public static final class InFlightSnapshot {
        public final int workerId;
        public final String taskId;
        public final long ageMs;

        private InFlightSnapshot(int workerId, String taskId, long ageMs) {
            this.workerId = workerId;
            this.taskId = taskId;
            this.ageMs = ageMs;
        }
    }

    /**
     * Returns inflight tasks grouped by worker.
     */
    public Map<Integer, List<InFlightSnapshot>> snapshotInFlight(long nowMs) {
        Map<Integer, List<InFlightSnapshot>> result = new LinkedHashMap<>();
        for (Map.Entry<String, ConcurrentHashMap<Integer, TaskAssignment>> entry : taskAssignments.entrySet()) {
            String taskId = entry.getKey();
            for (TaskAssignment assignment : entry.getValue().values()) {
                long age = Math.max(0L, nowMs - assignment.startMs);
                InFlightSnapshot snap = new InFlightSnapshot(assignment.workerId, taskId, age);
                result.computeIfAbsent(assignment.workerId, ignored -> new ArrayList<>()).add(snap);
            }
        }
        return result;
    }
     
    /**
     * Main loop that pulls messages and routes them to the correct handler.
     */
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message message = messageQueue.take();
                if(message instanceof Message.LeaseRenew lease)handelLeaseRenew(lease);
                else if(message instanceof Message.ProducerSubmitJob jobMsg)handleProducerSubmitJob(jobMsg);
                else if(message instanceof Message.ChatMessagePayload chatMessage)handleChatMessagePayload(chatMessage);
                else if(message instanceof Message.WorkerRequestTask requestTask)handelWorkerRequestTask(requestTask);
                else if(message instanceof Message.WorkerTaskResult result)handelWorkerTaskResult(result);
                else if(message instanceof Message.WorkerTaskProgress progress)handleWorkerTaskProgress(progress);
                long now = System.currentTimeMillis();
                if (now - lastReclaimCheck > 5_000L) {
                    reclaimExpiredTasks();
                    lastReclaimCheck = now;
                }
            } catch (Exception e) {
            }
        }
    }

    /**
     * Returns whether pending Tasks.
     */
    boolean hasPendingTasks() {
        return !taskQueue.isEmpty();
    }

    /**
     * Parses gradient Result.
     */
    private GradientResult parseGradientResult(String payload, int features) {
        if (payload == null || payload.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty gradient payload");
        }
        String[] parts = payload.split("\\|");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid gradient payload");
        }
        int count = Integer.parseInt(parts[0].trim());
        double lossSum = Double.parseDouble(parts[1].trim());
        double[] gradSum = new double[features];
        String gradRaw = null;
        for (int i = 2; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.startsWith("gradSum=")) {
                gradRaw = part.substring("gradSum=".length());
                break;
            }
        }
        if (gradRaw != null && !gradRaw.isEmpty()) {
            String[] values = gradRaw.split(",");
            int limit = Math.min(values.length, features);
            for (int i = 0; i < limit; i++) {
                gradSum[i] = Double.parseDouble(values[i].trim());
            }
        }
        return new GradientResult(count, lossSum, gradSum);
    }

    /**
     * Performs reclaim Expired Tasks.
     */
    private void reclaimExpiredTasks() {
        long now = System.currentTimeMillis();
        for (JobState jobState : jobs.values()) {
            for (TaskSeq task : jobState.getTasks()) {
                if (task.getState() != TaskState.RUNNING) {
                    continue;
                }
                int workerId = task.getWorkerId();
                boolean workerAlive = workerId != 0 && workers.containsKey(workerId);
                boolean leaseExpired = task.getLeaseUntil() > 0 && now > task.getLeaseUntil();
                TaskAssignment currentAssignment = null;
                ConcurrentHashMap<Integer, TaskAssignment> assignments = taskAssignments.get(task.getTaskId());
                if (assignments != null) {
                    currentAssignment = assignments.get(workerId);
                }
                long lastProgress = currentAssignment != null ? currentAssignment.lastProgressMs : 0L;
                if (lastProgress == 0L && currentAssignment != null) {
                    lastProgress = currentAssignment.startMs;
                }
                boolean progressStale = currentAssignment != null
                        && (now - lastProgress) > TASK_PROGRESS_TIMEOUT_MS;
                boolean longTimeout = currentAssignment != null
                        && (now - currentAssignment.startMs) > TASK_LONG_TIMEOUT_MS;
                // reclaim when worker dies, progress stops, or long timeout reached
                if (!workerAlive || progressStale || longTimeout || (leaseExpired && currentAssignment == null)) {
                    TaskAssignment assignment = removeAssignment(task.getTaskId(), workerId);
                    int perfWorkerId = assignment != null ? assignment.workerId : workerId;
                    long durationMs = assignment != null ? Math.max(1L, now - assignment.startMs) : -1L;
                    if (perfWorkerId > 0) {
                        WorkerPerf perf = workerPerf.computeIfAbsent(perfWorkerId, ignored -> new WorkerPerf());
                        perf.onFailed(now, durationMs);
                    }
                    speculativeTasks.remove(task.getTaskId());
                    task.setWorkerId(0);
                    task.setLeaseUntil(0L);
                    task.setResult("");
                    task.setState(TaskState.PENDING);
                    long delay = computeBackoffMs(task.getAttempts());
                    task.setNextEligibleAt(now + delay);
                    if (!taskQueue.offer(task)) {
                        System.err.println("Failed to re-queue task " + task.getTaskId());
                    } else {
                        System.out.println("Re-queued task " + task.getTaskId());
                    }
                }
            }
        }
    }

    /**
     * Exponential backoff capped at BACKOFF_MAX_MS.
     */
    private long computeBackoffMs(int attempts) {
        long backoff = (long) (BACKOFF_BASE_MS * Math.pow(2, Math.max(0, attempts - 1)));
        return Math.min(BACKOFF_MAX_MS, backoff);
    }

    /**
     * Records a new task assignment for performance timing.
     */
    private void recordAssignment(String taskId, int workerId) {
        long now = System.currentTimeMillis();
        taskAssignments
                .computeIfAbsent(taskId, ignored -> new ConcurrentHashMap<>())
                .putIfAbsent(workerId, new TaskAssignment(workerId, now));
        WorkerPerf perf = workerPerf.computeIfAbsent(workerId, ignored -> new WorkerPerf());
        perf.onAssigned(now);
    }

    /**
     * Removes an assignment entry and returns it, if present.
     */
    private TaskAssignment removeAssignment(String taskId, int workerId) {
        ConcurrentHashMap<Integer, TaskAssignment> map = taskAssignments.get(taskId);
        if (map == null) {
            return null;
        }
        TaskAssignment assignment = map.remove(workerId);
        if (map.isEmpty()) {
            taskAssignments.remove(taskId);
        }
        return assignment;
    }

    /**
     * Updates progress for a running task and refreshes its lease.
     */
    private void handleWorkerTaskProgress(Message.WorkerTaskProgress progress) {
        ConcurrentHashMap<Integer, TaskAssignment> map = taskAssignments.get(progress.getTaskId());
        if (map == null) {
            return;
        }
        TaskAssignment assignment = map.get(progress.getWorkerId());
        if (assignment == null) {
            return;
        }
        long now = System.currentTimeMillis();
        assignment.lastProgressMs = now;
        assignment.processed = progress.getProcessed();
        assignment.total = progress.getTotal();
        JobState jobState = jobs.get(progress.getTaskId().split("-T", 2)[0]);
        if (jobState != null) {
            TaskSeq task = jobState.getTask(progress.getTaskId());
            if (task != null) {
                task.setLeaseUntil(now + TASK_LEASE_MS);
            }
        }
    }

    /**
     * Finds a speculative task that is running too long.
     */
    private TaskSeq findSpeculativeTask(long nowMs, int requesterId) {
        for (JobState jobState : jobs.values()) {
            for (TaskSeq task : jobState.getTasks()) {
                if (task.getState() != TaskState.RUNNING) {
                    continue;
                }
                String taskId = task.getTaskId();
                if (speculativeTasks.contains(taskId)) {
                    continue;
                }
                int originalWorker = task.getWorkerId();
                if (originalWorker == requesterId) {
                    continue;
                }
                ConcurrentHashMap<Integer, TaskAssignment> assignments = taskAssignments.get(taskId);
                TaskAssignment assignment = assignments != null ? assignments.get(originalWorker) : null;
                if (assignment == null) {
                    continue;
                }
                if (nowMs - assignment.startMs >= TASK_SPECULATE_MS) {
                    speculativeTasks.add(taskId);
                    return task;
                }
            }
        }
        return null;
    }

    /**
     * Computes an adaptive slot count for dynamic load balancing.
     */
    private int computeEffectiveSlots(int workerId, int requestedSlots) {
        if (requestedSlots <= 1) {
            return Math.max(1, requestedSlots);
        }
        WorkerPerf perf = workerPerf.get(workerId);
        if (perf == null) {
            return requestedSlots;
        }
        WorkerPerfSnapshot snapshot = perf.snapshot(workerId);
        long avgMs = snapshot.avgMs;
        if (avgMs <= 0) {
            return requestedSlots;
        }
        long globalAvg = computeGlobalAvgMs();
        if (globalAvg <= 0) {
            return requestedSlots;
        }
        double ratio = (double) avgMs / (double) globalAvg;
        if (ratio >= 2.0) {
            return 1;
        }
        if (ratio >= 1.5) {
            return Math.max(1, requestedSlots / 2);
        }
        return requestedSlots;
    }

    /**
     * Returns true if this worker is considered slow.
     */
    private boolean isSlowWorker(int workerId) {
        WorkerPerf perf = workerPerf.get(workerId);
        if (perf == null) {
            return false;
        }
        WorkerPerfSnapshot snapshot = perf.snapshot(workerId);
        if (snapshot.avgMs <= 0) {
            return false;
        }
        long globalAvg = computeGlobalAvgMs();
        if (globalAvg <= 0) {
            return false;
        }
        return ((double) snapshot.avgMs / (double) globalAvg) >= 1.5;
    }

    /**
     * Returns true if other idle (not slow) workers exist.
     */
    private boolean hasIdleFastWorker(int currentWorkerId) {
        for (Map.Entry<Integer, WorkerInfo> entry : workers.entrySet()) {
            int workerId = entry.getKey();
            if (workerId == currentWorkerId) {
                continue;
            }
            if (isSlowWorker(workerId)) {
                continue;
            }
            WorkerPerf perf = workerPerf.get(workerId);
            if (perf == null) {
                return true;
            }
            WorkerPerfSnapshot snap = perf.snapshot(workerId);
            if (snap.inFlight == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Computes global average task duration across workers.
     */
    private long computeGlobalAvgMs() {
        long sum = 0L;
        long count = 0L;
        for (Map.Entry<Integer, WorkerPerf> entry : workerPerf.entrySet()) {
            WorkerPerfSnapshot snap = entry.getValue().snapshot(entry.getKey());
            if (snap.avgMs > 0) {
                sum += snap.avgMs;
                count++;
            }
        }
        if (count == 0) {
            return 0L;
        }
        return sum / count;
    }

    /**
     * Clears other inflight assignments for a task after it finishes.
     */
    private void clearOtherAssignments(String taskId, int winnerId, long nowMs) {
        ConcurrentHashMap<Integer, TaskAssignment> map = taskAssignments.get(taskId);
        if (map == null || map.isEmpty()) {
            return;
        }
        // Best effort cancel for duplicates still running elsewhere.
        sendCancel(taskId);
        for (Map.Entry<Integer, TaskAssignment> entry : new ArrayList<>(map.entrySet())) {
            int workerId = entry.getKey();
            if (workerId == winnerId) {
                continue;
            }
            TaskAssignment assignment = map.remove(workerId);
            if (assignment != null) {
                WorkerPerf perf = workerPerf.computeIfAbsent(workerId, ignored -> new WorkerPerf());
                perf.onLate(nowMs);
            }
        }
        if (map.isEmpty()) {
            taskAssignments.remove(taskId);
        }
    }

    /**
     * Sends a best effort cancel for a task.
     */
    private void sendCancel(String taskId) {
        try {
            toSendMessages.put(new Message.MasterCancelTask(taskId));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


   
    /**
     * Handles worker Task Result.
     */
    private void handelWorkerTaskResult(Message.WorkerTaskResult result){
        String taskId = result.getTaskId();
        String payload = result.getPayload();
        String jobId = taskId.split("-T", 2)[0];
        JobState job = jobs.get(jobId);
        if (job == null) {
            System.err.println("Job not fond for task " + taskId);
            return;
        }
        job.noteWorker(result.getWorkerId());
        TaskSeq task = job.getTask(taskId);
        if (task == null) {
            System.err.println("Task not found for task " + taskId);
            return;
        }
        long now = System.currentTimeMillis();
        TaskAssignment assignment = removeAssignment(taskId, result.getWorkerId());
        int perfWorkerId = assignment != null ? assignment.workerId : result.getWorkerId();
        long durationMs = assignment != null ? Math.max(1L, now - assignment.startMs) : -1L;
        WorkerPerf perf = workerPerf.computeIfAbsent(perfWorkerId, ignored -> new WorkerPerf());
        speculativeTasks.remove(taskId);
        if (payload != null && payload.startsWith("FAILED")) {
            if (task.getState() == TaskState.COMPLETED) {
                System.out.println("Ignoring failed result for completed task " + taskId);
                perf.onLate(now);
                return;
            }
            if (payload.contains("reason=DATAREF")) {
                failJob(job, "DATAREF");
                perf.onFailed(now, durationMs);
                clearOtherAssignments(taskId, result.getWorkerId(), now);
                return;
            }
            if (payload.contains("reason=CANCELLED")) {
                perf.onLate(now);
                clearOtherAssignments(taskId, result.getWorkerId(), now);
                return;
            }
            perf.onFailed(now, durationMs);
            task.setWorkerId(0);
            task.setLeaseUntil(0L);
            task.setResult("");
            task.setState(TaskState.PENDING);
            long delay = computeBackoffMs(task.getAttempts());
            task.setNextEligibleAt(System.currentTimeMillis() + delay);
            if (!taskQueue.offer(task)) {
                System.err.println("Failed to re-queue task " + taskId);
                return;
            }
            System.out.println("Re-queued failed task " + taskId);
            return;
        }


        if (task.getState() == TaskState.COMPLETED) {
            System.out.println("Ignoring duplicate result for completed task " + taskId);
            perf.onLate(now);
            return;
        }
        if (job.getJob().getStatus() == JobStatus.FAILD) {
            System.out.println("Ignoring result for failed job " + jobId);
            perf.onLate(now);
            return;
        }
        task.setWorkerId(0);
        task.setLeaseUntil(0L);
        job.setTaskState(TaskState.COMPLETED, taskId);
        job.setTaskResult(taskId, payload);
        job.setResult(payload);
        perf.onCompleted(now, durationMs);
        clearOtherAssignments(taskId, result.getWorkerId(), now);
        Job jobRef = job.getJob();
        if (jobRef.getJobType() == Message.JobType.GRADIEN_JOB && jobRef instanceof TrainGdBatchJob trainJob) {
            GradientResult grad = null;
            try {
                grad = parseGradientResult(payload, trainJob.getFeatures());
            } catch (RuntimeException e) {
                System.err.println("Failed to parse gradient result for task " + taskId);
            }
            if (trainJob.isAsync()) {
            if (grad != null) {
                trainJob.applyGradient(grad.count, grad.lossSum, grad.gradSum);
            }
            if (job.decrement() == 0) {
                jobRef.setStatus(JobStatus.COMPLETED);
                String resultPayload = appendJobRuntimeFields(job.buildResultPayload(), jobRef);
                Message.MasterJobResult resultMsg =
                        new Message.MasterJobResult(jobRef.getProducerId(),
                                jobRef.getJobId(), resultPayload);
                try {
                    toSendMessages.put(resultMsg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                jobResults.put(jobRef.getJobId(), resultPayload);
                persistCompletedJobs();
                }
                return;
            }
            if (grad != null) {
                trainJob.accumulateGradient(grad.count, grad.lossSum, grad.gradSum);
            }
            if (job.decrement() == 0) {
                TrainGdBatchJob.EpochUpdate update = trainJob.applyAccumulatedGradientWithMetrics();
                if (update != null) {
                    emitEpochMetrics(trainJob, job, update);
                }
                if (trainJob.hasMoreBatches()) {
                    enqueueNextTrainBatch(trainJob, job);
                    notifyWorkersTaskReady(trainJob.getJobId());
                } else {
                    jobRef.setStatus(JobStatus.COMPLETED);
                    String resultPayload = job.buildResultPayload();
                    resultPayload = appendJobRuntimeFields(resultPayload, jobRef);
                    Message.MasterJobResult resultMsg =
                            new Message.MasterJobResult(jobRef.getProducerId(),
                                    jobRef.getJobId(), resultPayload);
                    try {
                        toSendMessages.put(resultMsg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    jobResults.put(jobRef.getJobId(), resultPayload);
                    persistCompletedJobs();
                }
            }
            return;
        }
        if(job.decrement() == 0){
            Job completedJob = jobRef;
            completedJob.setStatus(JobStatus.COMPLETED);
            String resultPayload = appendJobRuntimeFields(job.buildResultPayload(), completedJob);
            Message.MasterJobResult resultMsg =
                    new Message.MasterJobResult(completedJob.getProducerId(),
                            completedJob.getJobId(), resultPayload);
            try {
                toSendMessages.put(resultMsg);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            jobResults.put(completedJob.getJobId(), resultPayload);
            persistCompletedJobs();
        }

        
    }
    /**
     * Handles a worker request for tasks by assigning up to the requested slots.
     */
    private void handelWorkerRequestTask(Message.WorkerRequestTask msg){ 
        reclaimExpiredTasks();
        int slots = computeEffectiveSlots(msg.getId(), msg.getSlots());
        int id = msg.getId();
        long now = System.currentTimeMillis();
        for(int i = 0; i < slots;++i){
            try {
                TaskSeq task = null;
                while (true) {
                    TaskSeq candidate = taskQueue.poll();
                    if (candidate == null) {
                        break;
                    }
                    if (candidate.getState() != TaskState.PENDING) {
                        continue;
                    }
                    if (candidate.getNextEligibleAt() > now) {
                        taskQueue.offer(candidate);
                        continue;
                    }
                    if (isSlowWorker(id) && hasIdleFastWorker(id)) {
                        taskQueue.offer(candidate);
                        break;
                    }
                    task = candidate;
                    break;
                }
                if (task == null) {
                    TaskSeq speculative = findSpeculativeTask(now, id);
                    if (speculative != null) {
                        JobState job = jobs.get(speculative.getJobId());
                        Job jobRef = job != null ? job.getJob() : null;
                        String payloadToSend = speculative.getPayload();
                        if (jobRef instanceof TrainGdBatchJob trainJob && trainJob.isAsync()) {
                            payloadToSend = appendWeightsToPayload(payloadToSend, trainJob.getWeights());
                        }
                        Message.MasterAssignTask message =
                                new Message.MasterAssignTask(speculative.getTaskId(),
                                        speculative.getJob(), payloadToSend, id);
                        recordAssignment(speculative.getTaskId(), id);
                        toSendMessages.put(message);
                        continue;
                    }
                    WorkerInfo worker = workers.get(id);
                    if (worker != null) {
                        synchronized (worker.getOutputStream()) {
                            new Message.MasterNoTask().toStream(worker.getOutputStream());
                            worker.getOutputStream().flush();
                        }
                    }
                    break;
                }
                JobState job = jobs.get(task.getJobId());
                Job jobRef = job != null ? job.getJob() : null;
                if (task.getAttempts() >= MAX_TASK_ATTEMPTS) {
                    System.err.println("Max attempts reached for task " + task.getTaskId());
                    if (job != null) {
                        failJob(job, "MAX_ATTEMPTS");
                    }
                    continue;
                }
                if (jobRef != null && jobRef.getStatus() != JobStatus.RUNNING) {
                    jobRef.setStatus(JobStatus.RUNNING);
                    Message.ChatMessagePayload running =
                            new Message.ChatMessagePayload(SystemRole.MASTER, 0,
                                    "JOB_STATE " + jobRef.getJobId() + " RUNNING");
                    toSendMessages.put(running);
                }
                task.setWorkerId(id);
                task.setLeaseUntil(System.currentTimeMillis() + TASK_LEASE_MS);
                task.incrementAttempts();
                task.setNextEligibleAt(0L);
                task.setState(TaskState.RUNNING);
                recordAssignment(task.getTaskId(), id);
                String payloadToSend = task.getPayload();
                if (jobRef instanceof TrainGdBatchJob trainJob && trainJob.isAsync()) {
                    payloadToSend = appendWeightsToPayload(payloadToSend, trainJob.getWeights());
                }
                Message.MasterAssignTask message = new Message.MasterAssignTask(task.getTaskId(),task.getJob(), payloadToSend,id);
                toSendMessages.put(message);
                
            } catch (Exception e) {
            }
        }

    }
    /**
     * Handles job submission messages from producers by creating and queuing tasks.
     */
    private void handleProducerSubmitJob(Message.ProducerSubmitJob jobMsg){
      
        Message.JobType jobType = jobMsg.getJobType();
        
        if(jobType == Message.JobType.SUM_JOB){
            String jobId = jobMsg.getJobId();
            Job newJob = new Job(jobId, jobMsg.getId(), jobType, jobMsg.getPayload());
            jobToTask(newJob);
        }
        if(jobType == Message.JobType.SUMSQ_JOB){
            String jobId = jobMsg.getJobId();
            Job newJob = new Job(jobId, jobMsg.getId(), jobType, jobMsg.getPayload());
            jobToTask(newJob);
        }
        if(jobType == Message.JobType.GRADIEN_JOB){
            String jobId = jobMsg.getJobId();
            TrainGdBatchJob newJob = new TrainGdBatchJob(jobId, jobMsg.getId(), jobMsg.getPayload());
            jobToTaskTrainGdBatch(newJob);
        }
      
    }

    /**
     * Chooses a chunk size for splitting a job into tasks based on worker count.
     */
    private BigInteger chooseChunkSize(BigInteger start, BigInteger end, int activeWorkers) {
        BigInteger totalItems = end.subtract(start).add(BigInteger.ONE);
        if (activeWorkers == 0) activeWorkers = 1;
        int desiredTasks = Math.max(1, activeWorkers * 10);
        BigInteger tasks = BigInteger.valueOf(desiredTasks);
        BigInteger[] div = totalItems.divideAndRemainder(tasks);
        BigInteger chunk = div[1].equals(BigInteger.ZERO) ? div[0] : div[0].add(BigInteger.ONE);
        if (chunk.compareTo(BigInteger.ONE) < 0) {
            return BigInteger.ONE;
        }
        return chunk;

    }
    /**
     * Splits a job into task segments and enqueues them for assignment.
     */
    private void jobToTask(Job job) {
       System.out.println("I m hier");
        String payload = job.getPayload();
        String[] str = payload.split("\\|");
        BigInteger start;
        BigInteger end;
        try {
            start = new BigInteger(str[0]);
            end = new BigInteger(str[1]);
        } catch (RuntimeException e) {
            System.err.println("Invalid job range: " + payload);
            Message.ChatMessagePayload error =
                    new Message.ChatMessagePayload(SystemRole.MASTER, 0,
                            "JOB_STATE " + job.getJobId() + " FAILED_INVALID_RANGE");
            try {
                toSendMessages.put(error);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        System.out.println("start " + start + " end " + end);
        BigInteger chunkSize = chooseChunkSize(start, end, workers.size());
        System.out.println("start " + start + " end " + end+" chunksize  "+chunkSize);
        if (chunkSize.compareTo(BigInteger.ZERO) <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        if (end.compareTo(start) < 0) {
            throw new IllegalArgumentException("end must be >= start");
        }

        int taskCount = 0;
        System.out.println("Before put: queue size = " + taskQueue.size());
        ConcurrentHashMap<String, TaskSeq> tasksMap = new ConcurrentHashMap<>();

        BigInteger s = start;
        while (s.compareTo(end) <= 0) {
            BigInteger e = s.add(chunkSize).subtract(BigInteger.ONE);
            if (e.compareTo(end) > 0) {
                e = end;
            }
            String taskId = job.getJobId() + "-T" + taskCount;
            TaskSeq task = new TaskSeq(job.getJobId(), taskId, 0, job.getJobType(), null, s + "|" + e, null);

            try{
                tasksMap.put(taskId, task);
                taskQueue.put(task);
            }catch(InterruptedException e1){
                System.err.println("Faild to put in taskQueue\n");
            }
            System.out.println("Created " + taskCount + " tasks");
            System.out.println("Queue size now: " + taskQueue.size());

            taskCount++;
            s = e.add(BigInteger.ONE);
        }
        jobs.put(job.getJobId(), new JobState(tasksMap, new AtomicInteger(taskCount), job));
        jobStartMs.put(job.getJobId(), System.currentTimeMillis());
        System.out.println("Tasks are Readyyyy !!!");
        notifyWorkersTaskReady(job.getJobId());
    }

    /**
     * Initializes a TrainGdBatch job by enqueuing the first batch only.
     */
    private void jobToTaskTrainGdBatch(TrainGdBatchJob job) {
        int samples = job.getSamples();
        int batchSize = job.getBatchSize();
        int epochs = job.getEpochs();
        if (samples <= 0 || batchSize <= 0 || epochs <= 0) {
            System.err.println("Invalid TrainGdBatch parameters for job " + job.getJobId());
            Message.ChatMessagePayload error =
                    new Message.ChatMessagePayload(SystemRole.MASTER, 0,
                            "JOB_STATE " + job.getJobId() + " FAILED_INVALID_PARAMS");
            try {
                toSendMessages.put(error);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        JobState jobState = new JobState(new ConcurrentHashMap<>(), new AtomicInteger(0), job);
        jobs.put(job.getJobId(), jobState);
        long now = System.currentTimeMillis();
        jobStartMs.put(job.getJobId(), now);
        if (job.isAsync()) {
            enqueueAllTrainTasks(job, jobState);
        } else {
            enqueueNextTrainBatch(job, jobState);
            epochStartMs.put(job.getJobId(), now);
        }
        notifyWorkersTaskReady(job.getJobId());
        System.out.println("Tasks are Readyyyy !!!");
    }

    /**
     * Builds a worker task payload for a single batch, using a key=value pipe format.
     */
    private String buildTrainTaskPayload(TrainGdBatchJob job, int epoch, int batchId, int start, int count, boolean includeWeights) {
        StringBuilder sb = new StringBuilder();
        sb.append("jobId=").append(job.getJobId());
        sb.append("|epoch=").append(epoch);
        sb.append("|batchId=").append(batchId);
        sb.append("|start=").append(start);
        sb.append("|count=").append(count);
        sb.append("|features=").append(job.getFeatures());
        sb.append("|model=").append(job.getModel());
        sb.append("|task=").append(job.getTask());
        sb.append("|dataRef=").append(job.getDataRef());
        sb.append("|hasHeader=").append(job.getHasHeader());
        sb.append("|csvLabel=").append(job.getCsvLabel());
        sb.append("|normalize=").append(job.getNormalize());
        if (includeWeights) {
            double[] weights = job.getWeights();
            if (weights != null && weights.length > 0) {
                sb.append("|w=");
                for (int i = 0; i < weights.length; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(weights[i]);
                }
            }
        }
        return sb.toString();
    }

    /**
     * Performs append Weights To Payload.
     */
    private String appendWeightsToPayload(String payload, double[] weights) {
        if (payload == null) {
            return "";
        }
        if (payload.contains("|w=")) {
            return payload;
        }
        if (weights == null || weights.length == 0) {
            return payload;
        }
        StringBuilder sb = new StringBuilder(payload);
        sb.append("|w=");
        for (int i = 0; i < weights.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(weights[i]);
        }
        return sb.toString();
    }

    /**
     * Enqueues next Train Batch.
     */
    private void enqueueNextTrainBatch(TrainGdBatchJob job, JobState jobState) {
        if (!job.hasMoreBatches()) {
            return;
        }
        int batchStart = job.getCurrentBatchStart();
        int batchCount = job.getCurrentBatchCount();
        if (batchCount <= 0) {
            System.err.println("Invalid batch range for job " + job.getJobId());
            return;
        }
        int activeWorkers = Math.max(1, workers.size());
        int chunkSize = (batchCount + activeWorkers - 1) / activeWorkers;
        int taskCount = 0;
        ConcurrentHashMap<String, TaskSeq> tasksMap = new ConcurrentHashMap<>();
        for (int offset = 0; offset < batchCount; offset += chunkSize) {
            int start = batchStart + offset;
            int count = Math.min(chunkSize, batchCount - offset);
            if (count <= 0) {
                continue;
            }
            String taskId = job.getJobId() + "-T" + taskCount;
            String payload = buildTrainTaskPayload(job, job.getCurrentEpoch(),
                    job.getCurrentBatchId(), start, count, true);
            TaskSeq task = new TaskSeq(job.getJobId(), taskId, 0,
                    Message.JobType.GRADIEN_JOB, null, payload, null);
            try {
                tasksMap.put(taskId, task);
                taskQueue.put(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            taskCount++;
        }
        jobState.resetTasks(tasksMap, taskCount);
        epochStartMs.put(job.getJobId(), System.currentTimeMillis());
    }

    /**
     * Enqueues all Train Tasks.
     */
    private void enqueueAllTrainTasks(TrainGdBatchJob job, JobState jobState) {
        int samples = job.getSamples();
        int batchSize = job.getBatchSize();
        int epochs = job.getEpochs();
        if (samples <= 0 || batchSize <= 0 || epochs <= 0) {
            return;
        }
        int batchesPerEpoch = (samples + batchSize - 1) / batchSize;
        int activeWorkers = Math.max(1, workers.size());
        int taskCount = 0;
        ConcurrentHashMap<String, TaskSeq> tasksMap = new ConcurrentHashMap<>();
        for (int epoch = 0; epoch < epochs; epoch++) {
            for (int batchId = 0; batchId < batchesPerEpoch; batchId++) {
                int batchStart = batchId * batchSize;
                int batchCount = Math.min(batchSize, samples - batchStart);
                if (batchCount <= 0) {
                    continue;
                }
                int chunkSize = (batchCount + activeWorkers - 1) / activeWorkers;
                for (int offset = 0; offset < batchCount; offset += chunkSize) {
                    int start = batchStart + offset;
                    int count = Math.min(chunkSize, batchCount - offset);
                    if (count <= 0) {
                        continue;
                    }
                    String taskId = job.getJobId() + "-T" + taskCount;
                    String payload = buildTrainTaskPayload(job, epoch, batchId, start, count, false);
                    TaskSeq task = new TaskSeq(job.getJobId(), taskId, 0,
                            Message.JobType.GRADIEN_JOB, null, payload, null);
                    try {
                        tasksMap.put(taskId, task);
                        taskQueue.put(task);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    taskCount++;
                }
            }
        }
        jobState.resetTasks(tasksMap, taskCount);
    }

    /**
     * Emits an EpochMetrics message to the producer when an epoch finishes.
     */
    private void emitEpochMetrics(TrainGdBatchJob job, JobState jobState, TrainGdBatchJob.EpochUpdate update) {
        if (update == null) {
            return;
        }
        long now = System.currentTimeMillis();
        long start = epochStartMs.getOrDefault(job.getJobId(), now);
        long durationMs = Math.max(1L, now - start);
        double throughput = update.count > 0 ? update.count / (durationMs / 1000.0) : 0.0;
        String weights = job.weightsAsCsv();
        Message.EpochMetrics metrics = new Message.EpochMetrics(
                job.getJobId(),
                job.getProducerId(),
                update.epoch,
                update.avgLoss,
                Double.NaN, // producer can compute accuracy from weights
                durationMs,
                throughput,
                jobState.workerCount(),
                job.isAsync() ? "async" : "sync",
                weights
        );
        epochStartMs.put(job.getJobId(), now);
        try {
            toSendMessages.put(metrics);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Appends runtime and worker info into a result payload string.
     */
    private String appendJobRuntimeFields(String payload, Job job) {
        StringBuilder sb = new StringBuilder(payload == null ? "" : payload);
        String jobId = job.getJobId();
        long wallMs = 0L;
        long start = jobStartMs.getOrDefault(jobId, System.currentTimeMillis());
        wallMs = Math.max(1L, System.currentTimeMillis() - start);
        if (sb.length() > 0) {
            sb.append("|");
        }
        int workersUsed = jobs.containsKey(jobId) ? jobs.get(jobId).workerCount() : workers.size();
        sb.append("wallMs=").append(wallMs);
        sb.append("|workers=").append(workersUsed);
        if (job instanceof TrainGdBatchJob trainJob) {
            int samples = trainJob.getSamples();
            int epochs = trainJob.getEpochs();
            double throughput = (samples * (long) epochs) / (wallMs / 1000.0);
            sb.append("|throughput=").append(throughput);
            sb.append("|mode=").append(trainJob.isAsync() ? "async" : "sync");
        }
        return sb.toString();
    }

    /**
     * Marks a job as failed and clears its pending tasks.
     */
    private void failJob(JobState jobState, String reason) {
        if (jobState == null || jobState.getJob() == null) {
            return;
        }
        Job jobRef = jobState.getJob();
        if (jobRef.getStatus() == JobStatus.FAILD || jobRef.getStatus() == JobStatus.COMPLETED) {
            return;
        }
        jobRef.setStatus(JobStatus.FAILD);
        for (TaskSeq task : jobState.getTasks()) {
            task.setWorkerId(0);
            task.setLeaseUntil(0L);
            task.setResult("");
            task.setState(TaskState.FAILED);
        }
        removePendingTasksForJob(jobRef.getJobId());
        Message.ChatMessagePayload failedMsg =
                new Message.ChatMessagePayload(SystemRole.MASTER, 0,
                        "JOB_STATE " + jobRef.getJobId() + " FAILED_" + reason);
        Message.MasterJobResult resultMsg =
                new Message.MasterJobResult(jobRef.getProducerId(), jobRef.getJobId(),
                        "FAILED|reason=" + reason);
        try {
            toSendMessages.put(failedMsg);
            toSendMessages.put(resultMsg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        jobResults.put(jobRef.getJobId(), "FAILED|reason=" + reason);
        persistCompletedJobs();
    }

    /**
     * Removes pending tasks for a job from the queue.
     */
    private void removePendingTasksForJob(String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            return;
        }
        List<TaskSeq> keep = new ArrayList<>();
        taskQueue.drainTo(keep);
        for (TaskSeq task : keep) {
            if (!jobId.equals(task.getJobId())) {
                taskQueue.offer(task);
            }
        }
    }

    /**
     * Notifies workers that tasks are available for a job.
     */
    private void notifyWorkersTaskReady(String jobId) {
        Message.ChatMessagePayload msg =
                new Message.ChatMessagePayload(SystemRole.MASTER, 0, "Tasks ready for job " + jobId);
        for (WorkerInfo worker : workers.values()) {
            try {
                synchronized (worker.getOutputStream()) {
                    msg.toStream(worker.getOutputStream());
                    worker.getOutputStream().flush();
                }
            } catch (Exception e) {
                System.err.println("Failed to send task-ready message to worker " + worker.getWorkerId());
            }
        }
    }
    


    /**
     * Handles incoming chat messages from workers or producers.
     */
    private void handleChatMessagePayload(Message.ChatMessagePayload chatMessage){
        
    }

    /**
     * Handles lease renewals from workers and producers and updates last-lease time.
     */
    private void handelLeaseRenew(Message.LeaseRenew lease){
        if(lease.getRole() == SystemRole.WORKER){
            WorkerInfo worker = workers.get(lease.getId());
            if(worker == null){
                System.err.println("Lease renew from unknown worker ID: " + lease.getId());
                return;
            }
            worker.lastLease = System.currentTimeMillis();
        }
        if(lease.getRole() == SystemRole.PRODUCER){
            ProducerInfo producer = producers.get(lease.getId());
            if(producer == null){
                System.err.println("Lease renew from unknown producer ID: " + lease.getId());
                return;
            }
            
            producer.lastLease = System.currentTimeMillis();
        
        }
        System.err.println("Lease renewed for " + lease.getRole() + " ID: " + lease.getId());
    }
  
 
}
