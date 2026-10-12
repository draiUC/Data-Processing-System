package dps;

/** Signals that a task could not be processed. Transient failures may be retried. */
public class ProcessingException extends Exception {
    private static final long serialVersionUID = 1L;

    private final boolean transientFailure;

    public ProcessingException(String message, boolean transientFailure) {
        super(message);
        this.transientFailure = transientFailure;
    }

    public boolean isTransient() {
        return transientFailure;
    }
}
