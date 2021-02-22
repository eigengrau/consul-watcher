package de.schattenkopie.consul.watcher;

import com.ecwid.consul.v1.catalog.model.CatalogService;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Stores a Consul service that has either been added or removed to the service catalog. */
@ToString
@AllArgsConstructor
@EqualsAndHashCode
public class ServiceUpdate {
  public final Type type;
  public final CatalogService service;

  /**
   * Represents a stage in the registration lifecycle of a service in the Consul service catalog.
   */
  public enum Type {
    ADDED,
    REMOVED
  }
}
