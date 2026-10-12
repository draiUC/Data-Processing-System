package dps;

/** Thrown when a worker waits for a task but the queue stays empty for the whole timeout. */
public class EmptyQueueException extends Exception {
    private static final long serialVersionUID = 1L;

    public EmptyQueueException(String message) {
        super(message);
    }
}
