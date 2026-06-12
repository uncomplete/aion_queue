package com.aion.error;

import java.util.HashMap;
import java.util.Map;

/**
 * Base exception for all Aion library errors. Carries a structured {@link ErrorCode} plus
 * subclass-specific typed fields contributed via {@link #contextMap()}.
 */
public abstract class AionException extends Exception {

  private final ErrorCode errorCode;

  protected AionException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public ErrorScope getScope() {
    return errorCode.getScope();
  }

  /** Subclasses override to contribute their typed fields to {@link #toStructuredLog()}. */
  protected Map<String, Object> contextMap() {
    return Map.of();
  }

  /** Returns a structured representation for logging/metrics. */
  public Map<String, Object> toStructuredLog() {
    Map<String, Object> log = new HashMap<>();
    log.put("errorCode", errorCode.getCode());
    log.put("errorName", errorCode.name());
    log.put("scope", errorCode.getScope().name());
    log.put("message", getMessage());
    log.putAll(contextMap());
    return log;
  }

  /**
   * Wrap an arbitrary throwable into an {@link AionException} so downstream code never has to
   * branch on raw {@link Throwable}. Uses {@link ErrorCode#SYSTEM_RESOURCE_EXHAUSTED} as the
   * synthetic code (SERVICE-scoped); the original throwable is preserved as the cause.
   */
  public static AionException fromUnchecked(Throwable t) {
    if (t instanceof AionException ae) {
      return ae;
    }
    return new SyntheticException(
        ErrorCode.SYSTEM_RESOURCE_EXHAUSTED,
        t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage(),
        t);
  }

  /** Concrete AionException used to wrap stray throwables that escape typed handlers. */
  private static final class SyntheticException extends AionException {
    SyntheticException(ErrorCode code, String message, Throwable cause) {
      super(code, message, cause);
    }
  }
}
