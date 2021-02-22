package de.schattenkopie.consul.watcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogServicesRequest;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("Given a Catalog Watcher")
class CatalogWatcherTest {

  final long verifyTimeoutMillis = 2000;
  @Captor ArgumentCaptor<Flow.Subscription> subscriptionCaptor;
  CatalogWatcher catalogWatcher;
  @Mock Flow.Subscriber<ServiceUpdate> subscriber;
  @Mock ConsulClient consulClient;
  @Mock ServiceWatcherFactory serviceWatcherFactory;
  @Mock ServiceWatcher serviceWatcher1;
  @Mock ServiceWatcher serviceWatcher2;

  @BeforeEach
  void subscribe() {
    catalogWatcher = new CatalogWatcher(consulClient, serviceWatcherFactory);
    catalogWatcher.subscribe(subscriber);
    verify(subscriber, timeout(verifyTimeoutMillis)).onSubscribe(subscriptionCaptor.capture());
    subscriptionCaptor.getValue().request(Long.MAX_VALUE);
  }

  @AfterEach
  void stopWatcher() {
    catalogWatcher.stop();
  }

  @Nested
  @DisplayName("When a Consul service is added")
  class ServiceAdded {
    @BeforeEach
    void addService() {
      when(serviceWatcherFactory.create("service1")).thenReturn(serviceWatcher1);
      Map<String, List<String>> services = Map.of("service1", List.of());
      Response<Map<String, List<String>>> response = new Response<>(services, 1L, true, 1L);
      when(consulClient.getCatalogServices(any(CatalogServicesRequest.class)))
          .thenAnswer(new AnswersWithDelay(250, new Returns(response)));
      catalogWatcher.start();
    }

    @Test
    @DisplayName("then a new service watcher is started")
    void thenWatcherIsCreated() {
      verify(serviceWatcher1, timeout(verifyTimeoutMillis)).start();
    }

    @Test
    @DisplayName("then it subscribes to updates from the service watcher")
    void thenItSubscribesToUpdates() {
      ArgumentCaptor<Flow.Subscriber<ServiceUpdate>> subscriberCaptor =
          ArgumentCaptor.forClass(Flow.Subscriber.class);
      verify(serviceWatcher1, timeout(verifyTimeoutMillis)).subscribe(subscriberCaptor.capture());
      Flow.Subscriber<ServiceUpdate> serviceWatcherSubscriber = subscriberCaptor.getValue();
      CatalogService service = new CatalogService();
      ServiceUpdate update = new ServiceUpdate(ServiceUpdate.Type.ADDED, service);
      serviceWatcherSubscriber.onNext(update);
      verify(subscriber, timeout(verifyTimeoutMillis)).onNext(update);
    }
  }

  @Nested
  @DisplayName("When a Consul service is removed")
  class ServiceRemoved {
    @BeforeEach
    void addService() {
      when(serviceWatcherFactory.create("service1")).thenReturn(serviceWatcher1);
      when(serviceWatcherFactory.create("service2")).thenReturn(serviceWatcher2);
      Map<String, List<String>> services1 = Map.of("service1", List.of(), "service2", List.of());
      Response<Map<String, List<String>>> response1 = new Response<>(services1, 1L, true, 1L);
      Map<String, List<String>> services2 = Map.of("service1", List.of());
      Response<Map<String, List<String>>> response2 = new Response<>(services2, 2L, true, 1L);
      when(consulClient.getCatalogServices(any(CatalogServicesRequest.class)))
          .thenAnswer(new AnswersWithDelay(250, new Returns(response1)))
          .thenAnswer(new AnswersWithDelay(250, new Returns(response2)));
      catalogWatcher.start();
    }

    @Test
    @DisplayName("then it stops the service watcher")
    void thenWatcherIsStopped() {
      InOrder inOrder = inOrder(serviceWatcher2);
      inOrder.verify(serviceWatcher2, timeout(verifyTimeoutMillis)).start();
      inOrder.verify(serviceWatcher2, timeout(verifyTimeoutMillis)).stop();
    }
  }
}
