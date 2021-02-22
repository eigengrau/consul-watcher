package de.schattenkopie.consul.watcher;

/** Creates {@link ServiceWatcher} instances. */
public interface ServiceWatcherFactory {
  /**
   * Creates a {@link ServiceWatcher} instance.
   *
   * @param serviceName name of the service to monitor
   * @return a {@link ServiceWatcher} that monitors the service
   */
  ServiceWatcher create(String serviceName);
}
