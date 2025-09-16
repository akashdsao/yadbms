package com.dbms.yadbms.common.serializer;

import static com.dbms.yadbms.common.utils.Constants.PAGE_SIZE;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Singleton wrapper around a single Kryo instance with a simple registration API. Call
 * register(...) during application startup (before any serialize/deserialize).
 */
public final class KryoSerializer {

  private static volatile KryoSerializer instance;
  private final Kryo kryo;

  public static KryoSerializer getInstance() {
    if (instance == null) {
      synchronized (KryoSerializer.class) {
        if (instance == null) {
          instance = new KryoSerializer();
        }
      }
    }
    return instance;
  }

  private KryoSerializer() {
    this.kryo = new Kryo();
    this.kryo.setRegistrationRequired(true);
    this.kryo.setReferences(false);
    this.kryo.setInstantiatorStrategy(
        new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
  }

  /** Access to the underlying Kryo (if you need to tweak advanced options). */
  public Kryo kryo() {
    return kryo;
  }

  /** Optional: set a custom ClassLoader before registrations. */
  public KryoSerializer setClassLoader(ClassLoader cl) {
    this.kryo.setClassLoader(cl != null ? cl : getClass().getClassLoader());
    return this;
  }

  /** Optional: toggle whether registration is required (default: true). */
  public KryoSerializer setRegistrationRequired(boolean required) {
    this.kryo.setRegistrationRequired(required);
    return this;
  }

  /** Optional: enable/disable object reference tracking (default: false). */
  public KryoSerializer setReferences(boolean refs) {
    this.kryo.setReferences(refs);
    return this;
  }

  // ---- Registration API (call at startup) ----

  /** Register a class with a fixed ID (smallest payloads, fastest). */
  public <T> KryoSerializer register(Class<T> type, int id) {
    this.kryo.register(type, id);
    return this;
  }

  /** Register a class with a custom serializer and fixed ID. */
  public <T> KryoSerializer register(Class<T> type, Serializer<T> serializer, int id) {
    this.kryo.register(type, serializer, id);
    return this;
  }

  /** Register a class without an explicit ID (Kryo assigns one; payload a bit larger). */
  public <T> KryoSerializer register(Class<T> type) {
    this.kryo.register(type);
    return this;
  }

  public synchronized byte[] toBytes(Object value) {
    try (ByteBufferOutput output = new ByteBufferOutput(PAGE_SIZE)) {
      kryo.writeObjectOrNull(output, value, value.getClass());
      return output.toBytes();
    }
  }

  public synchronized <T> T fromBytes(byte[] bytes, Class<T> type) {
    if (bytes == null) return null;
    try (Input in = new Input(bytes)) {
      @SuppressWarnings("unchecked")
      T obj = (T) kryo.readObjectOrNull(in, type);
      return obj;
    }
  }
}
