package de.schattenkopie.consul.watcher;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogServiceRequest;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;

/** SimpleServiceWatcher watches an individual Consul Catalog service. */
@RequiredArgsConstructor
@CommonsLog
public class SimpleServiceWatcher implements ServiceWatcher {
  @NonNull private final ConsulClient consulClient;
  @NonNull private final String serviceName;
  private final SubmissionPublisher<ServiceUpdate> catalogUpdatePublisher =
      new SubmissionPublisher<>();
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private long consulLastIndex;
  private Set<CatalogService> catalogServices = new HashSet<>();

  @Override
  public void start() {
    log.info(String.format("watching service %s", serviceName));
    executor.submit(this::iterate);
  }

  @Override
  public void stop() {
    log.info("stopping..");
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException exception) {
      log.warn("interrupted during shutdown", exception);
    }
    log.info("stopped");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super ServiceUpdate> subscriber) {
    catalogUpdatePublisher.subscribe(subscriber);
  }

  private void iterate() {
    updateBackends();
    executor.submit(this::iterate);
  }

  private void updateBackends() {
    log.debug("requesting service backends from catalog");
    long consulQueryWaitTime = 10;
    QueryParams consulQueryParams =
        QueryParams.Builder.builder()
            .setWaitTime(consulQueryWaitTime)
            .setIndex(consulLastIndex)
            .build();
    CatalogServiceRequest request =
        CatalogServiceRequest.newBuilder().setQueryParams(consulQueryParams).build();
    Response<List<CatalogService>> response = consulClient.getCatalogService(serviceName, request);

    if (consulLastIndex == response.getConsulIndex()) {
      log.debug("service catalog unmodified");
      return;
    }

    consulLastIndex = response.getConsulIndex();

    Set<CatalogService> servicesNow = new HashSet<>(response.getValue());
    Set<CatalogService> servicesThen = new HashSet<>(catalogServices);

    if (servicesNow.equals(servicesThen)) {
      log.debug("service backends unmodified");
      return;
    }

    log.debug("service backends changed");

    Set<CatalogService> backendsAdded = new HashSet<>(servicesNow);
    backendsAdded.removeAll(servicesThen);

    Set<CatalogService> backendsRemoved = new HashSet<>(servicesThen);
    backendsRemoved.removeAll(servicesNow);

    for (CatalogService service : backendsAdded) {
      log.debug(String.format("backend added: %s", service));
      catalogUpdatePublisher.submit(new ServiceUpdate(ServiceUpdate.Type.ADDED, service));
    }

    for (CatalogService service : backendsRemoved) {
      log.debug(String.format("backend removed: %s", service));
      catalogUpdatePublisher.submit(new ServiceUpdate(ServiceUpdate.Type.REMOVED, service));
    }

    catalogServices = new HashSet<>(servicesNow);
  }
}
