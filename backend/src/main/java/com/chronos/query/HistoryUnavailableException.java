package com.chronos.query;

/**
 * Thrown when a historical query targets a version, timestamp, or snapshot whose history
 * has already been purged below the safe GC low watermark.
 */
public class HistoryUnavailableException extends RuntimeException {

    public static final String ERROR_CODE = "HISTORY_UNAVAILABLE";

    private final String requestedSelector;
    private final String requestedValue;
    private final long earliestAvailableSeq;
    private final long earliestAvailableTimestamp;

    public HistoryUnavailableException(String message,
                                       String requestedSelector,
                                       String requestedValue,
                                       long earliestAvailableSeq,
                                       long earliestAvailableTimestamp) {
        super(message);
        this.requestedSelector = requestedSelector;
        this.requestedValue = requestedValue;
        this.earliestAvailableSeq = earliestAvailableSeq;
        this.earliestAvailableTimestamp = earliestAvailableTimestamp;
    }

    public String getErrorCode() {
        return ERROR_CODE;
    }

    public String getRequestedSelector() {
        return requestedSelector;
    }

    public String getRequestedValue() {
        return requestedValue;
    }

    public long getEarliestAvailableSeq() {
        return earliestAvailableSeq;
    }

    public long getEarliestAvailableTimestamp() {
        return earliestAvailableTimestamp;
    }
}

