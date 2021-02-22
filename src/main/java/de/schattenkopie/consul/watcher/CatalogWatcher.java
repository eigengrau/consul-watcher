package de.schattenkopie.consul.watcher;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogServicesRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;

/**
 * {@code CatalogWatcher} watches the Consul service catalog and notifies subscribers when services
 * are added or removed.
 *
 * <p>Updates are published to subscribers as {@link ServiceUpdate} objects.
 */
@CommonsLog
@RequiredArgsConstructor
public class CatalogWatcher implements Flow.Publisher<ServiceUpdate> {
  private final SubmissionPublisher<ServiceUpdate> serviceUpdatePublisher =
      new SubmissionPublisher<>();
  @NonNull private final ConsulClient consulClient;
  @NonNull private final ServiceWatcherFactory serviceWatcherFactory;
  private final Map<String, ServiceWatcher> serviceWatchers = new ConcurrentHashMap<>();
  private final ServiceUpdateSubscriber serviceUpdateSubscriber = new ServiceUpdateSubscriber();
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private Set<String> services = new HashSet<>();
  private long consulLastIndex;

  /** Begins monitoring the Consul service catalog and notify subscribers about updates. */
  public void start() {
    executor.submit(this::iterate);
  }

  /** Blocks until the catalog watcher has been stopped. */
  public void awaitTermination() {
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException exception) {
      log.warn("interrupted while waiting for termination");
    }
  }

  /**
   * Stops monitoring the Consul service catalog.
   *
   * <p>After stopping the catalog watcher, subscribers will no longer be notified of updates to the
   * service catalog.
   */
  public void stop() {
    log.info("stopping..");
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException exception) {
      log.warn("interrupted during shutdown", exception);
    }
    for (ServiceWatcher serviceWatcher : serviceWatchers.values()) {
      log.info("stopping service watcher..");
      serviceWatcher.stop();
      log.info("service watcher stopped");
    }
    log.info("stopped");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super ServiceUpdate> subscriber) {
    serviceUpdatePublisher.subscribe(subscriber);
  }

  private void iterate() {
    updateServiceList();
    executor.submit(this::iterate);
  }

  /** Long polls the Consul catalog API and informs subscribers about any changes. */
  private void updateServiceList() {
    long consulWaitTime = 10;
    CatalogServicesRequest request =
        CatalogServicesRequest.newBuilder()
            .setQueryParams(
                QueryParams.Builder.builder()
                    .setWaitTime(consulWaitTime)
                    .setIndex(consulLastIndex)
                    .build())
            .build();
    log.debug("requesting consul service catalog");
    Response<Map<String, List<String>>> response = consulClient.getCatalogServices(request);

    if (response.getConsulIndex() == consulLastIndex) {
      log.debug("consul catalog unmodified");
      return;
    }

    consulLastIndex = response.getConsulIndex();

    Set<String> servicesNow = response.getValue().keySet();
    Set<String> servicesThen = this.services;

    Set<String> buffer;

    buffer = new HashSet<>(servicesNow);
    buffer.removeAll(servicesThen);
    Set<String> addedServices = buffer;

    for (String service : addedServices) {
      log.debug(String.format("starting serviceWatcher for service %s", service));
      ServiceWatcher serviceWatcher = serviceWatcherFactory.create(service);
      serviceWatchers.put(service, serviceWatcher);
      serviceWatcher.subscribe(serviceUpdateSubscriber);
      serviceWatcher.start();
    }

    buffer = new HashSet<>(servicesThen);
    buffer.removeAll(servicesNow);
    Set<String> removedServices = buffer;

    for (String service : removedServices) {
      log.debug(String.format("stopping serviceWatcher for service %s", service));
      ServiceWatcher serviceWatcher = serviceWatchers.remove(service);
      serviceWatcher.stop();
    }

    this.services = servicesNow;
  }

  private class ServiceUpdateSubscriber implements Flow.Subscriber<ServiceUpdate> {

    @Override
    public void onSubscribe(Flow.Subscription subscription) {}

    @Override
    public void onNext(ServiceUpdate item) {
      serviceUpdatePublisher.submit(item);
    }

    @Override
    public void onError(Throwable throwable) {
      log.error("service-watcher subscription reported error", throwable);
    }

    @Override
    public void onComplete() {}
  }
}
