package org.apache.spark.sql.kafka010;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public enum KafkaConsumerClientRegistry {
    INSTANCE ;

    public static int MIN_RECEIVE_BUFFER_SIZE = 16 * 1024 * 1024 ;
    public static int FETCH_MAX_BYTE_CONFIG = 16 * 1024 * 1024 ;
    ConcurrentMap<String, ConsumerNetworkClient> cache  = new ConcurrentHashMap<>();

    public ConsumerNetworkClient getOrCreate(Collection<String> bootstreapServers,
                                             Properties config) {
        List<String> clone = new ArrayList<>(bootstreapServers);
        String sorted = clone.stream().sorted().collect(Collectors.joining());
        ConsumerNetworkClient c = cache.computeIfAbsent(sorted, bootstreapServerString -> {
            return create(sorted, config) ;
        });
        return c ;
    }

    private static ConsumerNetworkClient create(String bootstrapServer, Properties map ){
        map.put("key.deserializer", ByteArraySerializer.class.getName());
        map.put("value.deserializer", ByteArraySerializer.class.getName());
        map.put("bootstrap.servers", bootstrapServer);
        Object rcvBufferSize = map.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG);

        if(rcvBufferSize== null || Integer.parseInt(rcvBufferSize.toString()) > MIN_RECEIVE_BUFFER_SIZE) {
            map.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, MIN_RECEIVE_BUFFER_SIZE);
        }
        map.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 16 * 1024 * 1024);
        ConsumerConfig config = new ConsumerConfig(map);
        org.apache.kafka.common.metrics.Metrics metrics = new org.apache.kafka.common.metrics.Metrics();
        String metricGrpPrefix = map.get("consumer.id").toString();
        String clientId = map.get("consumer.id").toString();
        ApiVersions apiVersions = new ApiVersions();
        Time time = Time.SYSTEM;
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time);
        FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.emptySet(),
                metricGrpPrefix);
        Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);
        LogContext logContext = new LogContext("[Consumer clientId=" + clientId +  "]");
        SubscriptionState subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.NONE );
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        Metadata metadata = new ConsumerMetadata(retryBackoffMs,
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                subscriptions, logContext, clusterResourceListeners);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                Collections.singletonList(bootstrapServer),
                config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));

        metadata.bootstrap(addresses, time.milliseconds());
        NetworkClient netClient = new NetworkClient(
                new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                        metrics, time, metricGrpPrefix, channelBuilder, logContext),
                metadata, clientId, 100, // a fixed large enough value will suffice for max in-flight requests
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                ClientDnsLookup.forConfig(config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                time, true,
                apiVersions,
                throttleTimeSensor,
                logContext);

        ConsumerNetworkClient client = new ConsumerNetworkClient(
                logContext,
                netClient,
                metadata,
                time,
                retryBackoffMs,
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                heartbeatIntervalMs);

        return client;

    }
}
