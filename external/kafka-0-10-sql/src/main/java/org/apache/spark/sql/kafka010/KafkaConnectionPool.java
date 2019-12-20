/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kafka010;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Deprecated
class KafkaConnection {
    public final SocketChannel channel;
    public final String hostPort ;
    public final String clientId;
    public  final AtomicInteger correlationIdCounter ;
    public KafkaConnection(SocketChannel channel, String hostPort, String clientId) {
        this.channel = channel;
        this.hostPort = hostPort;
        this.clientId = clientId;
        this.correlationIdCounter = new AtomicInteger();
    }
}

@Deprecated
class KafkaConnectionPoolConfig {
    public final List<String> bootstrapServers;
    public final String connectionIdStart;
    public KafkaConnectionPoolConfig(List<String> bootstrapServers, String connectionIdStart) {
        this.bootstrapServers = Collections.unmodifiableList(bootstrapServers);
        this.connectionIdStart = connectionIdStart;
    }
}

@Deprecated
public class KafkaConnectionPool extends ConnectionPoolImpl<KafkaConnectionPoolConfig, KafkaConnection> {

    //private static AtomicInteger clientIdCounter = new AtomicInteger();

    private static ConnectionPool<KafkaConnection> INSTANCE = null ;

    private KafkaConnectionPool(int maxSize, KafkaConnectionPoolConfig kafkaConnectionPoolConfig) {
        super(maxSize, kafkaConnectionPoolConfig, new DefaultChannelCreator());
    }

    public synchronized static ConnectionPool<KafkaConnection> getOrCreate(int size, KafkaConnectionPoolConfig kafkaConnectionPoolConfig) {
        if(INSTANCE == null){
            INSTANCE = new KafkaConnectionPool(size, kafkaConnectionPoolConfig);
        }
        return INSTANCE;
    }

    private static class DefaultChannelCreator implements ConnectionCreator<KafkaConnectionPoolConfig, KafkaConnection> {
        @Override
        public KafkaConnection create(KafkaConnectionPoolConfig kafkaConnectionPoolConfig) throws IOException {
            List<String> currentServers = new ArrayList<>();
            currentServers.addAll(kafkaConnectionPoolConfig.bootstrapServers);
            Collections.shuffle(currentServers);
            Map<String, Exception> exceptions = new HashMap<>();
            for (String hostPortStr : currentServers) {
                try {
                    String[] hostPort = hostPortStr.split(":");
                    SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
                    socketChannel.configureBlocking(true);
                    return new KafkaConnection(socketChannel, hostPortStr, kafkaConnectionPoolConfig.connectionIdStart + "-" + UUID.randomUUID());
                } catch (IOException ex) {
                    exceptions.put(hostPortStr, ex);
                }
            }
            throw new IOException("Not able to connect to any host" +
                    kafkaConnectionPoolConfig.bootstrapServers.stream().collect(Collectors.joining(",")));
        }
    }
}
