package de.luh.vss.chat.common;

import java.io.DataInputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines message Type values.
 */
public enum MessageType {
	SERVICE_ERROR_RESPONSE(0, Message.ServiceErrorResponse.class), 
	SERVICE_REGISTRATION_REQUEST(1, Message.ServiceRegistrationRequest.class),
	SERVICE_REGISTRATION_RESPONSE(2, Message.ServiceRegistrationResponse.class),
	CHAT_MESSAGE_PAYLOAD(4, Message.ChatMessagePayload.class),
	PRODUCER_SUBMIT_JOB(5, Message.ProducerSubmitJob.class),
	WORKER_REQUEST_TASK(6, Message.WorkerRequestTask.class),
	MASTER_ASSIGN_TASK(7, Message.MasterAssignTask.class),
	WORKER_TASK_RESULT(8, Message.WorkerTaskResult.class),
	WORKER_TASK_PROGRESS(9, Message.WorkerTaskProgress.class),
	LEASE_RENEW(100, Message.LeaseRenew.class),
		MASTER_NO_TASK(10, Message.MasterNoTask.class),
		MASTER_JOB_RESULT(11, Message.MasterJobResult.class),
        EPOCH_METRICS(12, Message.EpochMetrics.class),
        MASTER_CANCEL_TASK(13, Message.MasterCancelTask.class);

	//MASTER_NO_TASK(9, Message.MasterNoTask.class);



	private final int msgType;

	private Constructor<? extends Message> constr;

	private static final Map<Integer, MessageType> lookup = new HashMap<Integer, MessageType>();
	static {
		for (final MessageType mt : MessageType.values()) {
			lookup.put(mt.msgType, mt);
		}
	}

	/**
	 * Creates a MessageType.
	 */
	private MessageType(int msgType, final Class<? extends Message> cls) {
		this.msgType = msgType;
		try {
			this.constr = cls.getConstructor(DataInputStream.class);
		} catch (NoSuchMethodException | SecurityException e) {
			System.err.println("Error while registering message type. "
					+ "Constructor from DataInputStream not present or accessible");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Performs msg Type.
	 */
	int msgType() {
		return msgType;
	}

	/**
	 * Performs from Int.
	 */
	public static MessageType fromInt(final int val) {
		return lookup.get(val);
	}

	/**
	 * Performs from Int.
	 */
	public static Message fromInt(final int val, final DataInputStream in) throws ReflectiveOperationException {
		final MessageType mt = fromInt(val);
		if (mt == null) {
			throw new IllegalStateException("Unknown message type " + val);
		}
		return mt.constr.newInstance(in);
	}

}
