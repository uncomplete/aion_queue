package com.aion.error;

/**
 * Classifies the blast radius of an error when it surfaces at a service boundary.
 *
 * <p>Read by boundary code (notably {@code OrdrSession}) to decide whether an error invalidates
 * just the offending command, the entire session, or the whole service process.
 */
public enum ErrorScope {
  /** Invalidates only the offending command. The session (e.g. gRPC stream) survives. */
  COMMAND,

  /** Invalidates the session. The stream is terminated; other sessions are unaffected. */
  SESSION,

  /**
   * Invalidates the whole service process. Never sent over a stream; logged and surfaced via health
   * check. Treated by stream-bound policies as SESSION (terminate the offending stream) with the
   * additional contract that the operator should be alerted.
   */
  SERVICE
}
