package de.schattenkopie.consul.watcher;

import com.ecwid.consul.v1.ConsulClient;
import java.util.concurrent.Flow;
import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;

/**
 * An example application which uses a {@link CatalogWatcher} to report Consul service catalog
 * updates to standard output.
 */
@CommonsLog
@RequiredArgsConstructor
public class ExampleApp {
  private final CatalogWatcher catalogWatcher;

  /**
   * Starts the application.
   *
   * @param args ignored
   */
  public static void main(String[] args) {
    ConsulClient consulClient = new ConsulClient();
    CatalogWatcher catalogWatcher =
        new CatalogWatcher(
            consulClient, serviceName -> new SimpleServiceWatcher(consulClient, serviceName));
    ExampleApp app = new ExampleApp(catalogWatcher);
    Runtime.getRuntime().addShutdownHook(new Thread(catalogWatcher::stop));
    app.runForever();
  }

  void runForever() {
    catalogWatcher.subscribe(new ServiceUpdateSubscriber());
    catalogWatcher.start();
    catalogWatcher.awaitTermination();
  }

  static class ServiceUpdateSubscriber implements Flow.Subscriber<ServiceUpdate> {
    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      log.info("now subscribed to catalog updates");
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ServiceUpdate item) {
      log.info(String.format("new catalog update: %s", item));
    }

    @Override
    public void onError(Throwable throwable) {
      log.error("catalog update subscription reported error", throwable);
    }

    @Override
    public void onComplete() {
      log.info("catalog update subscription complete");
    }
  }
}
