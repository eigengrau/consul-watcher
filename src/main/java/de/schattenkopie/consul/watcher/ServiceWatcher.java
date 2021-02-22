package de.schattenkopie.consul.watcher;

import java.util.concurrent.Flow;

/**
 * Monitors individual Consul catalog services and publishes notifications to subscribers when
 * service backends are modified.
 */
public interface ServiceWatcher extends Flow.Publisher<ServiceUpdate> {
  /** Starts monitoring the target service. */
  void start();

  /**
   * Stops monitoring the target service and will cause subscribers to no longer receive updates.
   *
   * <p>This returns immediately. For a limited duration, notifications may be published to
   * subscribers even after this method was called.
   */
  void stop();
}
