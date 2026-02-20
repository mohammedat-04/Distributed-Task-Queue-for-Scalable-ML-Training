package de.luh.vss.chat.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;


/**
 * Represents message.
 */
public abstract class Message {

	private static final String VERSION = "1.1";

	/**
	 * Defines job Type values.
	 */
	public enum JobType{
		SUM_JOB,
		SUMSQ_JOB,
		GRADIEN_JOB,
		ML_JOB;
	}
	/**
	 * Represents service Registration Request.
	 */
	public static class ServiceRegistrationRequest extends Message {
		private final SystemRole role;
		private final InetAddress address;
		private final int port;
		private final int id;

		/**
		 * Creates a ServiceRegistrationRequest.
		 */
		public ServiceRegistrationRequest(SystemRole role,int id,final InetAddress address, final int port) {
			this.role = role;
			this.id = id;
			this.address = address;
			this.port = port;
		}

		/**
		 * Creates a ServiceRegistrationRequest.
		 */
		public ServiceRegistrationRequest(final DataInputStream in) throws IOException {
			this.role = SystemRole.fromCode(in.readInt());
			this.id = in.readInt();
			this.address = InetAddress.getByName(in.readUTF());
			this.port = in.readInt();
		}

		/**
		 * Returns role.
		 */
		public SystemRole getRole() {
			return role;
		}
		/**
		 * Returns id.
		 */
		public int getId() {
			return id;
		}

		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {
			return MessageType.SERVICE_REGISTRATION_REQUEST;
		}

		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.SERVICE_REGISTRATION_REQUEST.msgType());
			out.writeInt(role.code);
			out.writeInt(id);
			out.writeUTF(address.getCanonicalHostName());
			out.writeInt(port);
			out.flush();

		}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {
			return "SERVICE_REGISTRATION_REQUEST (" + address.getCanonicalHostName() + ":" + port + ")";
		}
	}

	/**
	 * Represents service Registration Response.
	 */
	public static class ServiceRegistrationResponse extends Message {
		/**
		 * Creates a ServiceRegistrationResponse.
		 */
		public ServiceRegistrationResponse() {}
		/**
		 * Creates a ServiceRegistrationResponse.
		 */
		public ServiceRegistrationResponse(final DataInputStream in) {}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.SERVICE_REGISTRATION_RESPONSE;}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {out.writeInt(MessageType.SERVICE_REGISTRATION_RESPONSE.msgType());}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "SERVICE_REGISTRATION_RESPONSE ()";}
	}

	/**
	 * Represents service Error Response.
	 */
	public static class ServiceErrorResponse extends Message {
		private final String errorMsg;
		/**
		 * Creates a ServiceErrorResponse.
		 */
		public ServiceErrorResponse(final Exception e) {this.errorMsg = e.getMessage();}
		/**
		 * Creates a ServiceErrorResponse.
		 */
		public ServiceErrorResponse(final DataInputStream in) throws IOException {errorMsg = in.readUTF();}
		/**
		 * Creates a ServiceErrorResponse.
		 */
		public ServiceErrorResponse(final String e) {this.errorMsg = e;}
		/**
		 * Returns error Message.
		 */
		public String getErrorMessage() {return errorMsg;}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.SERVICE_ERROR_RESPONSE.msgType());
			out.writeUTF(errorMsg);
		}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.SERVICE_ERROR_RESPONSE;}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "SERVICE_ERROR_RESPONSE (" + errorMsg + ")";}
	}
	/**
	 * Represents chat Message Payload.
	 */
	public static class ChatMessagePayload extends Message {
		private final String msg;
		private final SystemRole role;
		private final int id;
		/**
		 * Creates a ChatMessagePayload.
		 */
		public ChatMessagePayload(SystemRole role,int id,final String msg) {this.msg = msg;this.role = role;this.id = id;}
		/**
		 * Creates a ChatMessagePayload.
		 */
		public ChatMessagePayload(final DataInputStream in) throws IOException {
			this.role = SystemRole.fromCode(in.readInt());
			this.id = in.readInt();
			this.msg = in.readUTF();
		}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.CHAT_MESSAGE_PAYLOAD.msgType());
			out.writeInt(role.code);
			out.writeInt(id);
			out.writeUTF(msg);
			out.flush();

		}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {
			return MessageType.CHAT_MESSAGE_PAYLOAD;
		}

		/**
		 * Returns message.
		 */
		public String getMessage() {
			return msg;
		}
		/**
		 * Returns role.
		 */
		public SystemRole getrole() {
			return role;
		}
		/**
		 * Returns id.
		 */
		public int getId() {	
			return id;
		}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {
			return "CHAT_MESSAGE_PAYLOAD ( + msg + )";
		}
	}

	/**
	 * Represents producer Submit Job.
	 */
	public static class ProducerSubmitJob extends Message {
		private final int id;
		private final String payload;
		private final JobType job;
		private final String jobId;
		/**
		 * Creates a ProducerSubmitJob.
		 */
		public ProducerSubmitJob(int id,JobType job,String payload, String jobId) {
			this.id = id;
			this.job = job;
			this.payload = payload;
			this.jobId = jobId;
		}
		/**
		 * Creates a ProducerSubmitJob.
		 */
		public ProducerSubmitJob(final DataInputStream in) throws IOException {
			this.id = in.readInt();
			this.job = JobType.valueOf(in.readUTF());
			this.payload = in.readUTF();
			this.jobId = in.readUTF();
		}
		/**
		 * Returns job Id.
		 */
		public String getJobId() {
			return jobId;
		}
		/**
		 * Returns job Type.
		 */
		public JobType getJobType() {
			return job;
		}
		/**
		 * Returns id.
		 */
		public int getId() {
			return id;
		}
		/**
		 * Returns payload.
		 */
		public String getPayload() {
			return payload;
		}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.PRODUCER_SUBMIT_JOB;}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.PRODUCER_SUBMIT_JOB.msgType());
			out.writeInt(id);
			out.writeUTF(job.name());
			out.writeUTF(payload);
			out.writeUTF(jobId);

		}
		
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "PRODUCER_SUBMIT_Job ()";}
	}

	/**
	 * Represents worker Request Task.
	 */
	public static class WorkerRequestTask extends Message {
		private int slots;
		private int id;
		/**
		 * Creates a WorkerRequestTask.
		 */
		public WorkerRequestTask(int id,int slot ) {
			this.id = id;
			this.slots = slot;
		}
		/**
		 * Creates a WorkerRequestTask.
		 */
		public WorkerRequestTask(final DataInputStream in) throws IOException {
			this.id = in.readInt();
			this.slots = in.readInt();
		}
		/**
		 * Returns slots.
		 */
		public int getSlots(){return slots;}
		/**
		 * Returns id.
		 */
		public int getId(){return id;}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.WORKER_REQUEST_TASK;
		}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.WORKER_REQUEST_TASK.msgType());
			out.writeInt(id);
			out.writeInt(slots);
			out.flush();
		}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "WORKER_REQUEST_TASK ()";}
		
	}
	/**
	 * Represents master Assign Task.
	 */
	public static class MasterAssignTask extends Message {
		private String taskId;
		private int workerId;
		private JobType job;
		private String payload;
		/**
		 * Creates a MasterAssignTask.
		 */
		public MasterAssignTask(String id,JobType job,String payload,int workerId) {
			this.taskId=id;
			this.workerId = workerId;
			this.job=job;
			this.payload= payload;
		}
		/**
		 * Creates a MasterAssignTask.
		 */
		public MasterAssignTask(final DataInputStream in) throws IOException {
			this.taskId = in.readUTF();
			this.job = JobType.valueOf(in.readUTF());
			this.payload = in.readUTF();
		}

		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.MASTER_ASSIGN_TASK;}
		

        /**
         * Returns job.
         */
        public JobType getJob() {
            return job;
        }
		/**
		 * Returns worker Id.
		 */
		public int getWorkerId(){
			return workerId;
		}
        /**
         * Returns task Id.
         */
        public String getTaskId() {
            return taskId;
        }
		

        /**
         * Returns payload.
         */
        public String getPayload() {
            return payload;
        }
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.MASTER_ASSIGN_TASK.msgType());
			out.writeUTF(taskId);
			out.writeUTF(job.name());
			out.writeUTF(payload);
			out.flush();
		}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "MASTER_ASSIGN_TASK ()";}
	}
	/**
	 * Represents worker Task Result.
	 */
	public static class WorkerTaskResult extends Message {
		String taskId;
		int id;
		String payloadResult;
		/**
		 * Creates a WorkerTaskResult.
		 */
		public WorkerTaskResult(int id,String taskId,String payload) {
			this.taskId = taskId;
			this.id= id;
			this.payloadResult = payload;
		}
		/**
		 * Creates a WorkerTaskResult.
		 */
		public WorkerTaskResult(final DataInputStream in) throws IOException {
			this.id = in.readInt();
			this.taskId = in.readUTF();
			this.payloadResult = in.readUTF();
		}
		/**
		 * Returns task Id.
		 */
		public String getTaskId(){return this.taskId;}
		/**
		 * Returns worker Id.
		 */
		public int getWorkerId(){return  this.id;}
		/**
		 * Returns payload.
		 */
		public String getPayload(){return this.payloadResult;}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.WORKER_TASK_RESULT;}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.WORKER_TASK_RESULT.msgType());
			out.writeInt(id);
			out.writeUTF(taskId);
			out.writeUTF(payloadResult);
			out.flush();
		}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "WORKER_TASK_RESULT ()";}
	}
	/**
	 * Represents worker task progress.
	 */
	public static class WorkerTaskProgress extends Message {
		private final int id;
		private final String taskId;
		private final int processed;
		private final int total;
		/**
		 * Creates a WorkerTaskProgress.
		 */
		public WorkerTaskProgress(int id, String taskId, int processed, int total) {
			this.id = id;
			this.taskId = taskId;
			this.processed = processed;
			this.total = total;
		}
		/**
		 * Creates a WorkerTaskProgress from stream.
		 */
		public WorkerTaskProgress(final DataInputStream in) throws IOException {
			this.id = in.readInt();
			this.taskId = in.readUTF();
			this.processed = in.readInt();
			this.total = in.readInt();
		}
		public int getWorkerId() { return id; }
		public String getTaskId() { return taskId; }
		public int getProcessed() { return processed; }
		public int getTotal() { return total; }
		@Override
		public MessageType getMessageType() { return MessageType.WORKER_TASK_PROGRESS; }
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.WORKER_TASK_PROGRESS.msgType());
			out.writeInt(id);
			out.writeUTF(taskId);
			out.writeInt(processed);
			out.writeInt(total);
			out.flush();
		}
		@Override
		public String toString() {return "WORKER_TASK_PROGRESS ()";}
	}
	/**
	 * Represents lease Renew.
	 */
	public static class LeaseRenew extends Message {
		private final SystemRole role;
		private final int id;
		/**
		 * Creates a LeaseRenew.
		 */
		public LeaseRenew(SystemRole role ,int id) {
			this.role = role; 
			this.id = id;
		}
		/**
		 * Creates a LeaseRenew.
		 */
		public LeaseRenew(final DataInputStream in) throws IOException {
			this.role = SystemRole.fromCode(in.readInt());
			this.id = in.readInt();
		}
		/**
		 * Returns message Type.
		 */
		@Override
		public MessageType getMessageType() {return MessageType.LEASE_RENEW;}
		/**
		 * Performs to Stream.
		 */
		@Override
		public void toStream(final DataOutputStream out) throws IOException {
			out.writeInt(MessageType.LEASE_RENEW.msgType());
			out.writeInt(role.code);
			out.writeInt(id);
			out.flush();
		}
		/**
		 * Returns role.
		 */
		public SystemRole getRole() {
			return role;
		}
		/**
		 * Returns id.
		 */
		public int getId() {
			return id;
		}
		
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "WORKER_LEASE_RENEW ()";}
	}
	/**
	 * Represents master No Task.
	 */
	public static class MasterNoTask extends Message {
    /**
     * Creates a MasterNoTask.
     */
    public MasterNoTask() {}
    /**
     * Creates a MasterNoTask.
     */
    public MasterNoTask(DataInputStream in) {}
    @Override public MessageType getMessageType() { return MessageType.MASTER_NO_TASK; }
	
    @Override public void toStream(DataOutputStream out) throws IOException {
        out.writeInt(MessageType.MASTER_NO_TASK.msgType());
		}
		
	}
	/**
	 * Represents master Job Result.
	 */
	public static class MasterJobResult extends Message {
		private final int producerId;
		private final String jobId;
		private final String result;
		/**
		 * Creates a MasterJobResult.
		 */
		public MasterJobResult(int producerId, String jobId, String result) {
			this.producerId = producerId;
			this.jobId = jobId;
			this.result = result;
		}
		/**
		 * Creates a MasterJobResult.
		 */
		public MasterJobResult(DataInputStream in) throws IOException {
			this.producerId = in.readInt();
			this.jobId = in.readUTF();
			this.result = in.readUTF();
		}
		/**
		 * Returns producer Id.
		 */
		public int getProducerId() { return producerId; }
		/**
		 * Returns job Id.
		 */
		public String getJobId() { return jobId; }
		/**
		 * Returns result.
		 */
		public String getResult() { return result; }
		/**
		 * Returns message Type.
	 */
	@Override
	public MessageType getMessageType() { return MessageType.MASTER_JOB_RESULT; }
		/**
		 * Performs to Stream.
	 */
	@Override
	public void toStream(DataOutputStream out) throws IOException {
			out.writeInt(MessageType.MASTER_JOB_RESULT.msgType());
			out.writeInt(producerId);
			out.writeUTF(jobId);
			out.writeUTF(result);
			out.flush();
	}
		/**
		 * Returns a string representation.
		 */
		@Override
		public String toString() {return "MASTER_JOB_RESULT ()";}
	}

	/**
	 * Represents a master cancel task message (best effort).
	 */
	public static class MasterCancelTask extends Message {
		private final String taskId;
		/**
		 * Creates a MasterCancelTask.
		 */
		public MasterCancelTask(String taskId) {
			this.taskId = taskId;
		}
		/**
		 * Creates a MasterCancelTask from stream.
		 */
		public MasterCancelTask(DataInputStream in) throws IOException {
			this.taskId = in.readUTF();
		}
		public String getTaskId() {
			return taskId;
		}
		@Override
		public MessageType getMessageType() {
			return MessageType.MASTER_CANCEL_TASK;
		}
		@Override
		public void toStream(DataOutputStream out) throws IOException {
			out.writeInt(MessageType.MASTER_CANCEL_TASK.msgType());
			out.writeUTF(taskId);
			out.flush();
		}
		@Override
		public String toString() {
			return "MASTER_CANCEL_TASK (" + taskId + ")";
		}
	}

	/**
	 * Represents per epoch training metrics emitted by the master.
	 */
	public static class EpochMetrics extends Message {
		private final String jobId;
		private final int producerId;
		private final int epoch;
		private final double loss;
		private final double accuracy;
		private final long durationMs;
		private final double throughput;
		private final int workers;
		private final String mode;
		private final String weightsCsv;

		/**
		 * Creates an EpochMetrics message.
		 */
		public EpochMetrics(String jobId,
		                    int producerId,
		                    int epoch,
		                    double loss,
		                    double accuracy,
		                    long durationMs,
		                    double throughput,
		                    int workers,
		                    String mode,
		                    String weightsCsv) {
			this.jobId = jobId;
			this.producerId = producerId;
			this.epoch = epoch;
			this.loss = loss;
			this.accuracy = accuracy;
			this.durationMs = durationMs;
			this.throughput = throughput;
			this.workers = workers;
			this.mode = mode == null ? "sync" : mode;
			this.weightsCsv = weightsCsv == null ? "" : weightsCsv;
		}

		/**
		 * Creates an EpochMetrics message from stream.
		 */
		public EpochMetrics(DataInputStream in) throws IOException {
			this.jobId = in.readUTF();
			this.producerId = in.readInt();
			this.epoch = in.readInt();
			this.loss = in.readDouble();
			this.accuracy = in.readDouble();
			this.durationMs = in.readLong();
			this.throughput = in.readDouble();
			this.workers = in.readInt();
			this.mode = in.readUTF();
			this.weightsCsv = in.readUTF();
		}

		public String getJobId() { return jobId; }
		public int getProducerId() { return producerId; }
		public int getEpoch() { return epoch; }
		public double getLoss() { return loss; }
		public double getAccuracy() { return accuracy; }
		public long getDurationMs() { return durationMs; }
		public double getThroughput() { return throughput; }
		public int getWorkers() { return workers; }
		public String getMode() { return mode; }
		public String getWeightsCsv() { return weightsCsv; }

		@Override
		public MessageType getMessageType() {
			return MessageType.EPOCH_METRICS;
		}

		@Override
		public void toStream(DataOutputStream out) throws IOException {
			out.writeInt(MessageType.EPOCH_METRICS.msgType());
			out.writeUTF(jobId);
			out.writeInt(producerId);
			out.writeInt(epoch);
			out.writeDouble(loss);
			out.writeDouble(accuracy);
			out.writeLong(durationMs);
			out.writeDouble(throughput);
			out.writeInt(workers);
			out.writeUTF(mode);
			out.writeUTF(weightsCsv);
			out.flush();
		}

		@Override
		public String toString() {
			return "EPOCH_METRICS (" + jobId + " e=" + epoch + ")";
		}
	}

	/**
	 * Parses value.
	 */
	public static Message parse(final DataInputStream in) throws IOException, ReflectiveOperationException {
		return MessageType.fromInt(in.readInt(), in);
	}

	/**
	 * Performs to Stream.
	 */
	public abstract void toStream(final DataOutputStream out) throws IOException;

	/**
	 * Returns message Type.
	 */
	public abstract MessageType getMessageType();
}
