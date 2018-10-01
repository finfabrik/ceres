package com.blokaly.ceres.common;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Configs {
  private static final Logger log = LoggerFactory.getLogger(Configs.class);



  private final Config javavm = ConfigFactory.systemProperties();

  private Config composite() {
    return convertSystemUnderscoreToDot()
        .withFallback(ConfigFactory.systemEnvironment())
        .withFallback(this.javavm)
        .withFallback(new Builder().withSecureConf().envAwareApp().build())
        .withFallback(ConfigFactory.parseResources("overrides.conf"))
        .withFallback(ConfigFactory.parseResources("defaults.conf"));
  }

  public static Config getConfig() {
    return new Configs().composite();
  }

  // This should return the current executing user path
  private String getExecutionDirectory() {
    return this.javavm.getString("user.dir");
  }

  public static final BiFunction<Config, String, String> STRING_EXTRACTOR = Config::getString;
  public static final BiFunction<Config, String, Boolean> BOOLEAN_EXTRACTOR = Config::getBoolean;
  public static final BiFunction<Config, String, Integer> INTEGER_EXTRACTOR = Config::getInt;

  public static Config convertSystemUnderscoreToDot() {
    Map<String, String> env = System.getenv();
    Properties props = new Properties();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      String propName = entry.getKey();
      if (propName.contains("_")) {
        String name = propName.replaceAll("_", "\\.").toLowerCase();
        props.setProperty(name, entry.getValue());
      }
    }
    if (!props.isEmpty()) {
      return ConfigFactory.parseProperties(props);
    } else {
      return ConfigFactory.empty();
    }
  }

  public static List<String> getStringList(final Config config, final String key) {
    final ConfigValueType type = config.getValue(key).valueType();

    switch (type) {
      case LIST:
        return config.getStringList(key);

      case STRING:
        final String value = config.getString(key);
        final List<String> list = new ArrayList<String>();
        final JsonArray array = new JsonParser().parse(value).getAsJsonArray();
        for (JsonElement jsonElement : array) {
          list.add(jsonElement.getAsString());
        }
        log.info("overriden env={} with value={}", key, list);
        return list;

      default:
        return Collections.emptyList();
    }
  }

  public static Map<String, Object> asMap(final Config config) {
    return config.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().unwrapped()));
  }

  public static <T> T getOrDefault(final Config config, final String path,
                                   final BiFunction<Config, String, T> extractor, final T defaultValue) {
    if (config.hasPath(path)) {
      return extractor.apply(config, path);
    }
    return defaultValue;
  }

  public static <T> T getOrDefault(final Config config, final String path,
                                   final BiFunction<Config, String, T> extractor, final Supplier<T> defaultSupplier) {
    if (config.hasPath(path)) {
      return extractor.apply(config, path);
    }
    return defaultSupplier.get();
  }

  private class Builder {
    private Config conf;

    private Builder() {
      log.info("Loading configs first row is highest priority, second row is fallback and so on");
    }

    private Builder withResource(final String resource) {
      this.conf = this.returnOrFallback(ConfigFactory.parseResources(resource));
      log.info("Loaded config file from resource ({})", resource);
      return this;
    }

    private Builder envAwareApp() {
      String env =
          Configs.this.javavm.hasPath("env") ? Configs.this.javavm.getString("env") : "local";
      String envFile = "application." + env + ".conf";
      return this.withResource(envFile).withResource("application.conf");
    }

    private Builder withSecureConf() {
      URL resource = ClassLoader.getSystemClassLoader().getResource("secure.conf");
      if (resource == null) {
        log.info("Attempted to load file secure.conf from classpath, but not found");
      } else {
        log.info("Loaded secure config file from path ({})", resource);
        this.conf = this.returnOrFallback(ConfigFactory.parseResources(resource.toString()));
      }

      return this;
    }

    private Config build() {
      // Resolve substitutions.
      this.conf = this.conf.resolve();
      if (log.isDebugEnabled()) {
        log.debug(
            "Logging properties. Make sure sensitive data such as passwords or secrets are not logged!");
        log.debug(this.conf.root().render(ConfigRenderOptions.concise().setFormatted(true)));
      }
      return this.conf;
    }

    private Config returnOrFallback(final Config config) {
      if (this.conf == null) {
        return config;
      }
      return this.conf.withFallback(config);
    }
  }

}
