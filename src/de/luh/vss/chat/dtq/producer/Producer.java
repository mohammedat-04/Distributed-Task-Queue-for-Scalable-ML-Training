package de.luh.vss.chat.dtq.producer;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.dtq.metrics.PlotGenerator;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CLI entry point for submitting jobs, resuming runs, and viewing results.
 */
public class Producer {
    private static final String C_RESET = "\u001B[0m";
    private static final String C_GREEN = "\u001B[32m";
    private static final String C_CYAN = "\u001B[36m";
    private static final String C_YELLOW = "\u001B[33m";
    private static final String C_RED = "\u001B[31m";
    private static int jobCounter = 0;
    /**
     * Generates the next job id for this producer.
     */
    private static String nextJobId(int producerId) {
        return "J" + producerId + (++jobCounter);
    }

    /**
     * Derives the next job counter from persisted job IDs.
     */
    private static int deriveJobCounter(int producerId, Set<String> jobIds) {
        String prefix = "J" + producerId;
        int max = 0;
        for (String jobId : jobIds) {
            if (jobId == null || !jobId.startsWith(prefix)) {
                continue;
            }
            String suffix = jobId.substring(prefix.length());
            try {
                int value = Integer.parseInt(suffix);
                if (value > max) {
                    max = value;
                }
            } catch (NumberFormatException ignored) {
                // ignore non-matching job IDs
            }
        }
        return max;
    }

    /**
     * Application entry point.
     */
    public static void main(String[] args) throws UnknownHostException, IOException {
        int id = Integer.parseInt(args[0]);
         
      
        ProducerConnection connection = new ProducerConnection("10.172.119.178", 44444, id) ;

        ProducerServerConnection serverConnection = new ProducerServerConnection(connection);
        Thread serverConnectionThread = new Thread(serverConnection);
        serverConnectionThread.start();
        try {
            serverConnection.awaitRegistration();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        ProducerReceiver producerReceiver = new ProducerReceiver(connection);
        Thread receiverThread  = new Thread(producerReceiver);
        receiverThread.start();

        Map<String, String> jobStates = new ConcurrentHashMap<>();
        Map<String, String> jobResults = new ConcurrentHashMap<>();
        Map<String, String> jobPayloads = new ConcurrentHashMap<>();
        Map<String, Long> jobSubmitTimes = new ConcurrentHashMap<>();
        Map<String, Long> jobDurations = new ConcurrentHashMap<>();
        ProducerStateStore stateStore = new ProducerStateStore(id);
        ProducerStateStore.State loadedState = stateStore.load();
        jobStates.putAll(loadedState.jobStates);
        jobResults.putAll(loadedState.jobResults);
        jobPayloads.putAll(loadedState.jobPayloads);
        jobSubmitTimes.putAll(loadedState.jobSubmitTimes);
        jobDurations.putAll(loadedState.jobDurations);
        jobCounter = Math.max(jobCounter, deriveJobCounter(id, jobStates.keySet()));
        Runnable stateSaver = () -> stateStore.save(jobStates, jobResults, jobPayloads, jobSubmitTimes, jobDurations);
        ProducerHandler producerHandler = new ProducerHandler(
                producerReceiver.getQueue(), jobStates, jobResults, jobPayloads,
                jobSubmitTimes, jobDurations, stateSaver);
        Thread handlerThread = new Thread(producerHandler);
        handlerThread.start();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            show();
            String line = scanner.nextLine();
            if (line == null) {
                continue;
            }
            String input = line.trim();
            if (input.isEmpty()) {
                continue;
            }
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("exit")) {
                break;
            }
            if (input.equalsIgnoreCase("help")) {
                printHelp();
                continue;
            }
            if (input.startsWith("sum ")) {
                String payload = input.substring(4).trim();
                if (!payload.contains("|")) {
                    System.out.println(C_RED + "Use: sum start|end" + C_RESET);
                    continue;
                }
                String jobId = nextJobId(id);
                connection.sendJob(Message.JobType.SUM_JOB, payload, jobId);
                jobStates.put(jobId, "SUBMITTED");
                jobSubmitTimes.put(jobId, System.currentTimeMillis());
                stateSaver.run();
                System.out.println(C_GREEN + "Job sent " + jobId + ": " + payload + C_RESET);
            } else if (input.startsWith("sumsq ")) {
                String payload = input.substring(6).trim();
                if (!payload.contains("|")) {
                    System.out.println(C_RED + "Use: sumsq start|end" + C_RESET);
                    continue;
                }
                String jobId = nextJobId(id);
                connection.sendJob(Message.JobType.SUMSQ_JOB, payload, jobId);
                jobStates.put(jobId, "SUBMITTED");
                jobSubmitTimes.put(jobId, System.currentTimeMillis());
                stateSaver.run();
                System.out.println(C_GREEN + "Job sent " + jobId + ": " + payload + C_RESET);
            } else if (input.equalsIgnoreCase("jobs")) {
                if (jobStates.isEmpty()) {
                    System.out.println(C_YELLOW + "No jobs submitted yet." + C_RESET);
                } else {
                    for (Map.Entry<String, String> entry : jobStates.entrySet()) {
                        String result = jobResults.get(entry.getKey());
                        Long duration = jobDurations.get(entry.getKey());
                        if (result == null || result.isEmpty()) {
                            System.out.println(entry.getKey() + " -> " + entry.getValue());
                        } else {
                            String displayResult = compactPayloadField(result, "weights");
                            String line1 = entry.getKey() + " -> " + entry.getValue() + " | result: " + displayResult;
                            if (duration != null) {
                                line1 += " | timeMs: " + duration;
                            }
                            System.out.println(line1);
                        }
                    }
                }
            } else if (input.startsWith("chat ")) {
                String msg = input.substring(6).trim();
                connection.sendChat(msg);
            } else if (input.startsWith("resume ")) {
                String rest = input.substring(8).trim();
                if (rest.isEmpty()) {
                    System.out.println(C_RED + "Use: resume jobId|key=value|..." + C_RESET);
                    continue;
                }
                String[] parts = rest.split("\\|");
                String sourceJobId = parts[0].trim();
                if (sourceJobId.isEmpty()) {
                    System.out.println(C_RED + "Missing jobId for resume" + C_RESET);
                    continue;
                }
                String resultPayload = jobResults.get(sourceJobId);
                if (resultPayload == null || resultPayload.isEmpty()) {
                    System.out.println(C_RED + "No result for jobId " + sourceJobId + C_RESET);
                    continue;
                }
                Map<String, String> resultFields = parsePayloadFields(resultPayload);
                String weights = resultFields.get("weights");
                if (weights == null || weights.isEmpty()) {
                    System.out.println(C_RED + "No weights found for jobId " + sourceJobId + C_RESET);
                    continue;
                }
                Map<String, String> baseFields = parsePayloadFields(jobPayloads.get(sourceJobId));
                Map<String, String> overrides = new HashMap<>();
                for (int i = 1; i < parts.length; i++) {
                    String token = parts[i].trim();
                    if (token.isEmpty()) {
                        continue;
                    }
                    String[] kv = token.split("=", 2);
                    if (kv.length != 2) {
                        continue;
                    }
                    overrides.put(kv[0].trim(), kv[1].trim());
                }
                String resumeJobId = overrides.get("jobId");
                if (resumeJobId == null || resumeJobId.isEmpty()) {
                    resumeJobId = nextJobId(id);
                }
                baseFields.putAll(overrides);
                baseFields.put("jobId", resumeJobId);
                baseFields.put("init", "weights");
                baseFields.put("w", weights);
                baseFields.putIfAbsent("type", "TrainGdBatchJob");
                String normalizedPayload = buildTrainPayload(baseFields);
                connection.sendJob(Message.JobType.GRADIEN_JOB, normalizedPayload, resumeJobId);
                jobStates.put(resumeJobId, "SUBMITTED");
                jobPayloads.put(resumeJobId, normalizedPayload);
                jobSubmitTimes.put(resumeJobId, System.currentTimeMillis());
                stateSaver.run();
                String displayPayload = compactPayloadField(normalizedPayload, "w");
                System.out.println(C_GREEN + "Job sent " + resumeJobId + ": " + displayPayload + C_RESET);
            }else if(input.equalsIgnoreCase("plot")){
                PlotGenerator.generateSpeedupChart(null);
                PlotGenerator.generateSyncVsAsyncChart(null);
                continue;
            }else if(input.startsWith("train ")){
                String rawPayload = input.substring(7).trim();
                if (rawPayload.isEmpty()) {
                    System.out.println(C_RED + "Use: train key=value|key=value|..." + C_RESET);
                    continue;
                }
                Map<String, String> fields = new HashMap<>();
                String[] parts = rawPayload.split("\\|");
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
                String jobId = fields.get("jobId");
                if (jobId == null || jobId.isEmpty()) {
                    jobId = nextJobId(id);
                }
                fields.put("jobId", jobId);
                fields.putIfAbsent("type", "TrainGdBatchJob");
                String[] requiredKeys = new String[]{
                        "model", "task", "samples", "features", "epochs",
                        "batchSize", "lr", "seed", "dataRef"
                };
                boolean missing = false;
                for (String key : requiredKeys) {
                    String value = fields.get(key);
                    if (value == null || value.isEmpty()) {
                        missing = true;
                        System.out.println(C_RED + "Missing required field: " + key + C_RESET);
                    }
                }
                if (missing) {
                    continue;
                }
                String wFile = fields.remove("wFile");
                if (wFile != null && !wFile.isEmpty()) {
                    String weights = readWeightsFile(wFile);
                    if (weights == null || weights.isEmpty()) {
                        System.out.println(C_RED + "Failed to read wFile: " + wFile + C_RESET);
                        continue;
                    }
                    fields.put("w", weights);
                    fields.putIfAbsent("init", "weights");
                }
                String normalizedPayload = buildTrainPayload(fields);
                connection.sendJob(Message.JobType.GRADIEN_JOB, normalizedPayload, jobId);
                jobStates.put(jobId, "SUBMITTED");
                jobPayloads.put(jobId, normalizedPayload);
                jobSubmitTimes.put(jobId, System.currentTimeMillis());
                stateSaver.run();
                String displayPayload = compactPayloadField(normalizedPayload, "w");
                System.out.println(C_GREEN + "Job sent " + jobId + ": " + displayPayload + C_RESET);
            }else {
                connection.sendChat(input);
            }
            
        }

    }
    /**
     * Performs show.
     */
    public static void show(){
        System.out.println(C_CYAN + "------- DTQ Producer -------" + C_RESET);
        System.out.println(C_YELLOW + "Type help for commands" + C_RESET);


    }
    /**
     * Performs print Help.
     */
    private static void printHelp() {
        System.out.println(C_CYAN + "Commands:" + C_RESET);
        System.out.println("  chat <message>   send a chat message");
        System.out.println("  sum <start|end>  submit a SUM job");
        System.out.println("  sumsq <start|end>  submit a SUMSQ job");
        System.out.println("  train <key=value|...>  submit a TrainGdBatch job (supports wFile=...)");
        System.out.println("  resume <jobId|key=value|...>  resume from last weights");
        System.out.println("  jobs             list submitted jobs and states");
        System.out.println("  plot             render speedup and sync-vs-async charts from logs");
        System.out.println("  exit             quit");
        System.out.println("  help             show this help");
    }

    /**
     * Builds train Payload.
     */
    private static String buildTrainPayload(Map<String, String> fields) {
        String[] orderedKeys = new String[]{
                "type", "jobId", "model", "task", "samples", "features", "epochs",
                "batchSize", "lr", "seed", "dataRef", "testRef", "hasHeader", "csvLabel",
                "normalize", "init", "w"
        };
        Set<String> used = new HashSet<>();
        StringBuilder sb = new StringBuilder();
        for (String key : orderedKeys) {
            appendField(sb, used, key, fields.get(key));
        }
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            appendField(sb, used, entry.getKey(), entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Performs append Field.
     */
    private static void appendField(StringBuilder sb, Set<String> used, String key, String value) {
        if (value == null || value.isEmpty() || used.contains(key)) {
            return;
        }
        if (sb.length() > 0) {
            sb.append('|');
        }
        sb.append(key).append('=').append(value);
        used.add(key);
    }

    /**
     * Performs compact Payload Field.
     */
    private static String compactPayloadField(String payload, String key) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }
        String token = key + "=";
        int startKey = payload.indexOf(token);
        if (startKey < 0) {
            return payload;
        }
        int startValue = startKey + token.length();
        int endValue = payload.indexOf('|', startValue);
        if (endValue < 0) {
            endValue = payload.length();
        }
        String value = payload.substring(startValue, endValue).trim();
        if (value.length() <= 120) {
            return payload;
        }
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == ',') {
                count++;
            }
        }
        if (!value.isEmpty()) {
            count += 1;
        }
        String replacement = count + " params";
        return payload.substring(0, startValue) + replacement + payload.substring(endValue);
    }

    /**
     * Parses payload Fields.
     */
    private static Map<String, String> parsePayloadFields(String payload) {
        Map<String, String> fields = new HashMap<>();
        if (payload == null || payload.isEmpty()) {
            return fields;
        }
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
        return fields;
    }

    /**
     * Performs read Weights File.
     */
    private static String readWeightsFile(String pathRef) {
        String ref = pathRef.trim();
        if (ref.startsWith("file:")) {
            ref = ref.substring("file:".length());
        }
        Path path = Paths.get(ref).normalize();
        if (!Files.exists(path)) {
            return null;
        }
        try {
            String raw = Files.readString(path, StandardCharsets.UTF_8);
            String[] tokens = raw.trim().split("[,\\s]+");
            StringBuilder sb = new StringBuilder();
            for (String token : tokens) {
                if (token.isEmpty()) {
                    continue;
                }
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(token);
            }
            return sb.toString();
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Validates that a required dataRef exists on disk.
     */
    private static boolean validateDataRef(String dataRef, String label) {
        if (dataRef == null || dataRef.isEmpty()) {
            System.out.println(C_RED + "Missing required field: " + label + C_RESET);
            return false;
        }
        Path resolved = resolveDataRef(dataRef);
        if (!Files.exists(resolved)) {
            System.out.println(C_RED + "Invalid " + label + ": " + dataRef + " (resolved to " + resolved + ")" + C_RESET);
            return false;
        }
        return true;
    }

    /**
     * Validates optional dataRef if provided.
     */
    private static boolean validateOptionalDataRef(String dataRef, String label) {
        if (dataRef == null || dataRef.isEmpty()) {
            return true;
        }
        Path resolved = resolveDataRef(dataRef);
        if (!Files.exists(resolved)) {
            System.out.println(C_RED + "Invalid " + label + ": " + dataRef + " (resolved to " + resolved + ")" + C_RESET);
            return false;
        }
        return true;
    }

    /**
     * Resolves a dataRef using the same fallback logic as workers.
     */
    private static Path resolveDataRef(String dataRef) {
        String ref = dataRef.trim();
        if (ref.startsWith("file:")) {
            ref = ref.substring("file:".length());
        }
        Path path = Paths.get(ref).normalize();
        if (Files.exists(path)) {
            return path;
        }
        Path fallback = Paths.get("src/de/luh/vss/chat/dtq/worker").resolve(path).normalize();
        if (Files.exists(fallback)) {
            return fallback;
        }
        Path fallbackData = Paths.get("src/de/luh/vss/chat/dtq/worker/data")
                .resolve(path.getFileName() != null ? path.getFileName().toString() : path.toString())
                .normalize();
        if (Files.exists(fallbackData)) {
            return fallbackData;
        }
        return path;
    }
}
