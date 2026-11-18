package com.chronos.query;

/**
 * Selector modes supported by the Chronos temporal query engine.
 */
public enum QueryMode {
    CURRENT,
    AS_OF_VERSION,
    AS_OF_TIME,
    AS_OF_SNAPSHOT,
    AS_OF_VALID_TIME,
    BITEMPORAL
}

