package dps;

/** A unit of work placed on the shared queue. Immutable, so it is safe to hand between threads. */
public record Task(int id, String payload) {

    /** Builds a deterministic sample record so Java and Go produce comparable output. */
    public static Task generate(int id) {
        return new Task(id, String.format("sensor-%03d,reading=%d", id, (id * 37 + 11) % 100));
    }
}
