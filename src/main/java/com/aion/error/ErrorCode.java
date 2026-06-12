package com.aion.error;

/**
 * Enumeration of error codes in the system. Each code carries a numeric identifier, a {@link
 * ErrorScope} that describes its blast radius at a service boundary, and a human-readable
 * description.
 *
 * <p>This is a trimmed copy carrying only the codes referenced by the {@code com.aion.aion} queue
 * classes and their exceptions.
 */
public enum ErrorCode {

  // System errors (5000-5999)
  SYSTEM_RESOURCE_EXHAUSTED(5001, ErrorScope.SERVICE, "System resources exhausted"),

  // Aion queue errors (3900-3999)
  QUEUE_END_OF_STREAM(
      3900, ErrorScope.SERVICE, "Queue reader reached end-of-stream (writer closed and drained)"),
  QUEUE_READ(3901, ErrorScope.SERVICE, "Queue read failure (e.g. blocking read timed out)"),
  QUEUE_WRITE(
      3902,
      ErrorScope.SERVICE,
      "Queue write failure (e.g. timed out waiting for capacity under backpressure)"),
  QUEUE_MONOTONICITY_VIOLATION(
      3903, ErrorScope.SERVICE, "Queue write violated strict monotonic timestamp contract");

  private final int code;
  private final ErrorScope scope;
  private final String description;

  ErrorCode(int code, ErrorScope scope, String description) {
    this.code = code;
    this.scope = scope;
    this.description = description;
  }

  public int getCode() {
    return code;
  }

  public ErrorScope getScope() {
    return scope;
  }

  public String getDescription() {
    return description;
  }
}
