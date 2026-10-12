package dps;

/** Outcome of processing one task. Immutable. */
public record Result(int taskId, String worker, String input, String output,
                     long durationMs, int attempts, Status status, String error) {

    public enum Status { SUCCESS, FAILED }

    public static final String CSV_HEADER =
            "task_id,worker,input,output,duration_ms,attempts,status,error";

    public String toCsv() {
        return String.join(",", String.valueOf(taskId), worker, quote(input), quote(output),
                String.valueOf(durationMs), String.valueOf(attempts), status.name(), quote(error));
    }

    private static String quote(String s) {
        return "\"" + (s == null ? "" : s.replace("\"", "\"\"")) + "\"";
    }
}
