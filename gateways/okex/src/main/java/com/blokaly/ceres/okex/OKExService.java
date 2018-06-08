package com.blokaly.ceres.okex;

import com.blokaly.ceres.binding.BootstrapService;
import com.blokaly.ceres.binding.CeresModule;
import com.blokaly.ceres.network.WSConnectionListener;
import com.blokaly.ceres.system.CommonConfigs;
import com.blokaly.ceres.system.Services;
import com.blokaly.ceres.common.Source;
import com.blokaly.ceres.kafka.HBProducer;
import com.blokaly.ceres.kafka.KafkaCommonModule;
import com.blokaly.ceres.kafka.KafkaStreamModule;
import com.blokaly.ceres.kafka.ToBProducer;
import com.blokaly.ceres.okex.event.ChannelEvent;
import com.blokaly.ceres.okex.event.EventAdapter;
import com.blokaly.ceres.orderbook.PriceBasedOrderBook;
import com.blokaly.ceres.web.HandlerModule;
import com.blokaly.ceres.web.UndertowModule;
import com.blokaly.ceres.web.handlers.HealthCheckHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import io.undertow.Undertow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OKExService extends BootstrapService {
  private static final Pattern SPOT_SUB_PATTERN =Pattern.compile("ok_sub_spot_([a-z]+)_([a-z]+)_depth");
  private final OKExClientProvider provider;
  private final KafkaStreams streams;
  private final Undertow undertow;

  @Inject
  public OKExService(OKExClientProvider provider,
                     @Named("Throttled") KafkaStreams streams,
                     Undertow undertow) {
    this.provider = provider;
    this.streams = streams;
    this.undertow = undertow;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("starting OKEx client...");
    provider.start();

    waitFor(3);
    LOGGER.info("starting kafka streams...");
    streams.start();

    LOGGER.info("Web server starting...");
    undertow.start();
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Web server stopping...");
    undertow.stop();

    LOGGER.info("stopping OKEx client...");
    provider.stop();

    LOGGER.info("stopping kafka streams...");
    streams.close();
  }

  public static class OKExModule extends CeresModule {

    @Override
    protected void configure() {
      this.install(new UndertowModule(new HandlerModule() {

        @Override
        protected void configureHandlers() {
          this.bindHandler().to(HealthCheckHandler.class);
        }
      }));
      expose(Undertow.class);

      install(new KafkaCommonModule());
      install(new KafkaStreamModule());
      bindExpose(ToBProducer.class);
      bind(HBProducer.class).asEagerSingleton();
      expose(StreamsBuilder.class).annotatedWith(Names.named("Throttled"));
      expose(KafkaStreams.class).annotatedWith(Names.named("Throttled"));

      bind(MessageHandler.class).to(MessageHandlerImpl.class).in(Singleton.class);
      bindExpose(OKExClientProvider.class).asEagerSingleton();
      bind(WSConnectionListener.class).to(OKExClientProvider.class);
      bindExpose(OKExClient.class).toProvider(OKExClientProvider.class);
    }

    @Provides
    @Exposed
    public URI provideUri(Config config) throws Exception {
      return new URI(config.getString("app.ws.url"));
    }

    @Provides
    @Singleton
    @Exposed
    public Gson provideGson() {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(ChannelEvent.class, new EventAdapter());
      return builder.create();
    }

    @Provides
    @Singleton
    @Exposed
    public Map<String, PriceBasedOrderBook> provideOrderBooks(Config config) {
      List<String> channels = config.getStringList("channels");
      String source = Source.valueOf(config.getString(CommonConfigs.APP_SOURCE).toUpperCase()).getCode();

      return channels.stream().collect(Collectors.<String , String, PriceBasedOrderBook>toMap(chan->chan, chan -> {
        Matcher matcher = SPOT_SUB_PATTERN.matcher(chan);
        if (matcher.matches()) {
          String pair = matcher.group(1) + matcher.group(2);
          return new PriceBasedOrderBook(pair, pair + "." + source);
        } else {
          throw new IllegalArgumentException("channel pattern is wrong: " + chan);
        }
      }));
    }
  }

  public static void main(String[] args) {
    Services.start(new OKExModule());
  }
}
