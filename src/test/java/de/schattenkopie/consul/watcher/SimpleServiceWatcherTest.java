package de.schattenkopie.consul.watcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogServiceRequest;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import java.util.List;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("Given a service watcher")
class SimpleServiceWatcherTest {
  final long verifyTimeoutMillis = 5000;
  final CatalogService service1 = new CatalogService();
  final CatalogService service2 = new CatalogService();
  final CatalogService service3 = new CatalogService();
  @Mock Flow.Subscriber<ServiceUpdate> subscriber;
  @Captor ArgumentCaptor<ServiceUpdate> updateCaptor;
  @Captor ArgumentCaptor<Flow.Subscription> subscriptionCaptor;
  SimpleServiceWatcher serviceWatcher;
  @Mock ConsulClient consulClient;

  SimpleServiceWatcherTest() {
    service1.setServiceName("test-service");
    service1.setServiceAddress("host1");
    service2.setServiceName("test-service");
    service2.setServiceAddress("host2");
    service3.setServiceName("test-service");
    service3.setServiceAddress("host3");
  }

  @BeforeEach
  void init() {
    serviceWatcher = new SimpleServiceWatcher(consulClient, "test-service");
    serviceWatcher.subscribe(subscriber);
    verify(subscriber, timeout(verifyTimeoutMillis)).onSubscribe(subscriptionCaptor.capture());
    Flow.Subscription subscription = subscriptionCaptor.getValue();
    subscription.request(Long.MAX_VALUE);
  }

  @AfterEach
  void stopWatcher() {
    serviceWatcher.stop();
  }

  @Nested
  @DisplayName("When a service appears")
  class ServiceAdded {
    @BeforeEach
    void init() {
      List<CatalogService> services = List.of(service1, service2, service3);
      Response<List<CatalogService>> response = new Response<>(services, 1L, true, 1L);
      doAnswer(new AnswersWithDelay(250, new Returns(response)))
          .when(consulClient)
          .getCatalogService(any(), any(CatalogServiceRequest.class));
      serviceWatcher.start();
    }

    @Test
    @DisplayName("then it publishes an update to subscribers")
    void thenUpdateIsPublished() {
      verify(subscriber, timeout(verifyTimeoutMillis).times(3)).onNext(updateCaptor.capture());
      List<ServiceUpdate> updates = updateCaptor.getAllValues();
      assertEquals(
          List.of(
              new ServiceUpdate(ServiceUpdate.Type.ADDED, service1),
              new ServiceUpdate(ServiceUpdate.Type.ADDED, service2),
              new ServiceUpdate(ServiceUpdate.Type.ADDED, service3)),
          updates);
    }
  }

  @Nested
  @DisplayName("when another backend appears")
  class BackendAdded {
    @BeforeEach
    void init() {
      Response<List<CatalogService>> response1 =
          new Response<>(List.of(service1, service2), 1L, true, 1L);
      Response<List<CatalogService>> response2 =
          new Response<>(List.of(service1, service2, service3), 2L, true, 1L);
      doAnswer(new AnswersWithDelay(250, new Returns(response1)))
          .doAnswer(new AnswersWithDelay(250, new Returns(response2)))
          .when(consulClient)
          .getCatalogService(any(), any(CatalogServiceRequest.class));
      serviceWatcher.start();
    }

    @Test
    @DisplayName("then it publishes an update to subscribers")
    void thenAnUpdateIsPublished() {
      verify(subscriber, timeout(verifyTimeoutMillis).times(3)).onNext(updateCaptor.capture());
      List<ServiceUpdate> updates = updateCaptor.getAllValues();
      assertEquals(
          List.of(
              new ServiceUpdate(ServiceUpdate.Type.ADDED, service1),
              new ServiceUpdate(ServiceUpdate.Type.ADDED, service2),
              new ServiceUpdate(ServiceUpdate.Type.ADDED, service3)),
          updates);
    }
  }
}
