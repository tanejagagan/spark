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
import java.util.stream.Collectors;

@Deprecated
public class ConnectionPoolImpl<I, T> implements ConnectionPool<T> {
    private final int maxSize;
    private final I inputMetadata;
    private final ConnectionCreator<I, T> creator;
    private List<T> availableConnectionPool = new ArrayList<>();
    private List<T> usedConnections = new ArrayList<>();
    private List<T> toRecover = new ArrayList<>();

    public ConnectionPoolImpl(int maxSize, I inputMetadata, ConnectionCreator<I, T> creator) {
        this.maxSize = maxSize;
        this.inputMetadata = inputMetadata;
        this.creator = creator;
    }

    public synchronized T getConnection() throws InterruptedException, IOException {
        while (usedConnections.size() == maxSize) {
            wait();
        }
        if (availableConnectionPool.size() == 0) {
            T channel = creator.create(inputMetadata);
            availableConnectionPool.add(channel);
        }
        T socket = availableConnectionPool.remove(availableConnectionPool.size() - 1);
        usedConnections.add(socket);
        return socket;
    }

    public synchronized boolean releaseConnection(T socket) {
        boolean res;
        if (toRecover.contains(socket)) {
            res = toRecover.remove(socket);
        } else {
            availableConnectionPool.add(socket);
            res = usedConnections.remove(socket);
        }
        notifyAll();
        return res;
    }

    public synchronized void markForRecovery(T socketChannel) {
        toRecover.add(socketChannel);
    }


    public int getSize() {
        return availableConnectionPool.size() + usedConnections.size();
    }


    private static class DefaultChannelCreator implements ConnectionCreator<List<String>, SocketChannel> {
        @Override
        public SocketChannel create(List<String> bootstrapServers) throws IOException {
            List<String> currentServers = new ArrayList<>();
            currentServers.addAll(bootstrapServers);
            Collections.shuffle(currentServers);
            Map<String, Exception> exceptions = new HashMap<>();
            for (String hostPortStr : currentServers) {
                try {
                    String[] hostPort = hostPortStr.split(":");
                    SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
                    return socketChannel;
                } catch (IOException ex) {
                    exceptions.put(hostPortStr, ex);
                }
            }
            throw new IOException("Not able to connect to any host" +
                    bootstrapServers.stream().collect(Collectors.joining(",")));
        }
    }
}
