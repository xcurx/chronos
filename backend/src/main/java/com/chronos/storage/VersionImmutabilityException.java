package com.chronos.storage;

/**
 * Thrown when an operation attempts to violate Chronos version or snapshot immutability.
 */
public class VersionImmutabilityException extends IllegalStateException {

    public VersionImmutabilityException(String message) {
        super(message);
    }
}

