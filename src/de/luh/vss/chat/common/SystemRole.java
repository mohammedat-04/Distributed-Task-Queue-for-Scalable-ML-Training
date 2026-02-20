package de.luh.vss.chat.common;
/* Role of a node in the system (encoded as an int in the protocol). */
public enum SystemRole {
	 /* Scheduler/coordinator node. */ MASTER(0),
    /* Executes tasks assigned by master. */ WORKER(1),
    /* Submits jobs/tasks to master. */ PRODUCER(2);

	/*Integer code sent over the network. */
	public final int code;

    /**
     * Creates a SystemRole.
     */
    SystemRole(int code) { this.code = code; }

	/* Convert wire code to enum value. */
    public static SystemRole fromCode(int c) {
        if (c == 0) return MASTER;
		if (c == 1) return WORKER;
		if (c == 2) return PRODUCER;
        throw new IllegalArgumentException("Unknown role code " + c);
    }

}