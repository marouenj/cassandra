/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.service.pager.AbstractQueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ServerConnection;

/**
 * Represents the state related to a given query.
 */
public class QueryState
{
    private final ClientState clientState;
    private volatile UUID preparedTracingSession;

    private final PiggyBack metadata = new PiggyBack();

    public QueryState(ClientState clientState)
    {
        this.clientState = clientState;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls());
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    /**
     * This clock guarantees that updates for the same QueryState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        return clientState.getTimestamp();
    }

    public boolean traceNextQuery()
    {
        if (preparedTracingSession != null)
        {
            return true;
        }

        double traceProbability = StorageService.instance.getTraceProbability();
        return traceProbability != 0 && ThreadLocalRandom.current().nextDouble() < traceProbability;
    }

    public void prepareTracingSession(UUID sessionId)
    {
        this.preparedTracingSession = sessionId;
    }

    public void createTracingSession()
    {
        UUID session = this.preparedTracingSession;
        if (session == null)
        {
            Tracing.instance.newSession();
        }
        else
        {
            Tracing.instance.newSession(session);
            this.preparedTracingSession = null;
        }
    }

    public InetAddress getClientAddress()
    {
        return clientState.isInternal
             ? null
             : clientState.getRemoteAddress().getAddress();
    }

    public PiggyBack getMetadataForConsistencyWithCallback() {
        return metadata;
    }

    public static class PiggyBack {

        private Message.Dispatcher dispatcher;
        private ChannelHandlerContext ctx;
        private Message.Request request;
        private int streamId;
        private ServerConnection connection;
        private Message.Type type;
        private QueryOptions queryOptions;
        private UUID tracingId;
        private SelectStatement selectStatement;
        private SelectStatement.Pager pager;
        private int nowInSec;
        private int userLimit;
        private AbstractQueryPager.Pager transformationPager;
        private SinglePartitionReadCommand.Group group;

        public Message.Dispatcher getDispatcher() {
            return dispatcher;
        }

        public void setDispatcher(Message.Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public ChannelHandlerContext getCtx() {
            return this.ctx;
        }

        public void setCtx(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        public Message.Request getRequest() {
            return request;
        }

        public void setRequest(Message.Request request) {
            this.request = request;
        }

        public int getStreamId() {
            return streamId;
        }

        public void setStreamId(int streamId) {
            this.streamId = streamId;
        }

        public ServerConnection getConnection() {
            return connection;
        }

        public void setConnection(ServerConnection connection) {
            this.connection = connection;
        }

        public Message.Type getType() {
            return type;
        }

        public void setType(Message.Type type) {
            this.type = type;
        }

        public QueryOptions getQueryOptions() {
            return queryOptions;
        }

        public void setQueryOptions(QueryOptions queryOptions) {
            this.queryOptions = queryOptions;
        }

        public UUID getTracingId() {
            return tracingId;
        }

        public void setTracingId(UUID tracingId) {
            this.tracingId = tracingId;
        }

        public SelectStatement getSelectStatement() {
            return selectStatement;
        }

        public void setSelectStatement(SelectStatement selectStatement) {
            this.selectStatement = selectStatement;
        }

        public SelectStatement.Pager getPager() {
            return pager;
        }

        public void setPager(SelectStatement.Pager pager) {
            this.pager = pager;
        }

        public int getNowInSec() {
            return nowInSec;
        }

        public void setNowInSec(int nowInSec) {
            this.nowInSec = nowInSec;
        }

        public int getUserLimit() {
            return userLimit;
        }

        public void setUserLimit(int userLimit) {
            this.userLimit = userLimit;
        }

        public AbstractQueryPager.Pager getTransformationPager() {
            return transformationPager;
        }

        public void setTransformationPager(AbstractQueryPager.Pager transformationPager) {
            this.transformationPager = transformationPager;
        }

        public SinglePartitionReadCommand.Group getGroup() {
            return group;
        }

        public void setGroup(SinglePartitionReadCommand.Group group) {
            this.group = group;
        }
    }
}
