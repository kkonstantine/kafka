/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COOP;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedAssignment;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedWorkerState;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for managing assignments
 * to workers.
 */
public final class WorkerCoordinator extends AbstractCoordinator implements Closeable {
    // Currently doesn't support multiple task assignment strategies, so we just fill in a default value
    public static final String DEFAULT_SUBPROTOCOL = "default";

    private final Logger log;
    private final String restUrl;
    private final ConfigBackingStore configStorage;
    private ExtendedAssignment assignmentSnapshot;
    private AtomicReference<ClusterConfigState> configSnapshot;
    private final WorkerRebalanceListener listener;
    private final ConnectProtocolCompatibility protocolCompatibility;
    private AtomicReference<LeaderState> leaderState;

    private boolean rejoinRequested;
    private final ConnectAssignor assignor;

    /**
     * Initialize the coordination manager.
     */
    public WorkerCoordinator(LogContext logContext,
                             ConsumerNetworkClient client,
                             String groupId,
                             int rebalanceTimeoutMs,
                             int sessionTimeoutMs,
                             int heartbeatIntervalMs,
                             Metrics metrics,
                             String metricGrpPrefix,
                             Time time,
                             long retryBackoffMs,
                             String restUrl,
                             ConfigBackingStore configStorage,
                             WorkerRebalanceListener listener,
                             ConnectProtocolCompatibility protocolCompatibility) {
        super(logContext,
              client,
              groupId,
              Optional.empty(),
              rebalanceTimeoutMs,
              sessionTimeoutMs,
              heartbeatIntervalMs,
              metrics,
              metricGrpPrefix,
              time,
              retryBackoffMs);
        this.log = logContext.logger(WorkerCoordinator.class);
        this.restUrl = restUrl;
        this.configStorage = configStorage;
        this.configSnapshot = new AtomicReference<>();
        this.leaderState = new AtomicReference<>();
        this.assignmentSnapshot = null;
        new WorkerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.listener = listener;
        this.rejoinRequested = false;
        this.protocolCompatibility = protocolCompatibility;
        this.assignor = new ConnectAssignor(logContext);
    }

    @Override
    public void requestRejoin() {
        rejoinRequested = true;
    }

    @Override
    public String protocolType() {
        return "connect";
    }

    // expose for tests
    @Override
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        return super.ensureCoordinatorReady(timer);
    }

    public void poll(long timeout) {
        // poll for io until the timeout expires
        final long start = time.milliseconds();
        long now = start;
        long remaining;

        do {
            if (coordinatorUnknown()) {
                ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
                now = time.milliseconds();
            }

            if (rejoinNeededOrPending()) {
                ensureActiveGroup();
                now = time.milliseconds();
            }

            pollHeartbeat(now);

            long elapsed = now - start;
            remaining = timeout - elapsed;

            // Note that because the network client is shared with the background heartbeat thread,
            // we do not want to block in poll longer than the time to the next heartbeat.
            long pollTimeout = Math.min(Math.max(0, remaining), timeToNextHeartbeat(now));
            client.poll(time.timer(pollTimeout));

            now = time.milliseconds();
            elapsed = now - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);
    }

    @Override
    public JoinGroupRequestData.JoinGroupRequestProtocolSet metadata() {
        configSnapshot.set(configStorage.snapshot());
        ExtendedWorkerState workerState = new ExtendedWorkerState(restUrl, configSnapshot.get().offset(), assignmentSnapshot);
        ByteBuffer metadata;
        switch (protocolCompatibility) {
            case STRICT:
                metadata = ConnectProtocol.serializeMetadata(workerState);
                return new JoinGroupRequestData.JoinGroupRequestProtocolSet(Collections.singleton(
                        new JoinGroupRequestData.JoinGroupRequestProtocol()
                                .setName(protocolCompatibility.protocol())
                                .setMetadata(metadata.array()))
                        .iterator());
            case COMPAT:
                return new JoinGroupRequestData.JoinGroupRequestProtocolSet(Arrays.asList(
                        new JoinGroupRequestData.JoinGroupRequestProtocol()
                                .setName(protocolCompatibility.protocol())
                                .setMetadata(IncrementalCooperativeConnectProtocol.serializeMetadata(workerState).array()),
                        new JoinGroupRequestData.JoinGroupRequestProtocol()
                                .setName(protocolCompatibility.protocol())
                                .setMetadata(ConnectProtocol.serializeMetadata(workerState).array()))
                        .iterator());
            case COOP:
                return new JoinGroupRequestData.JoinGroupRequestProtocolSet(Collections.singleton(
                        new JoinGroupRequestData.JoinGroupRequestProtocol()
                                .setName(protocolCompatibility.protocol())
                                .setMetadata(IncrementalCooperativeConnectProtocol.serializeMetadata(workerState).array()))
                        .iterator());
            default:
                throw new IllegalStateException("Unknown Connect protocol compatibility mode " + protocolCompatibility);
        }
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        ExtendedAssignment newAssignment = protocolCompatibility == COOP
                ? IncrementalCooperativeConnectProtocol.deserializeAssignment(memberAssignment)
                : new ExtendedAssignment(ConnectProtocol.deserializeAssignment(memberAssignment),
                                         Collections.emptyList(),
                                         Collections.emptyList());
        // At this point we always consider ourselves to be a member of the cluster, even if there was an assignment
        // error (the leader couldn't make the assignment) or we are behind the config and cannot yet work on our assigned
        // tasks. It's the responsibility of the code driving this process to decide how to react (e.g. trying to get
        // up to date, try to rejoin again, leaving the group and backing off, etc.).
        rejoinRequested = false;
        //TODO: don't call on empty revoked connectors and tasks
        listener.onRevoked(newAssignment.leader(), newAssignment.revokedConnectors(),
                           newAssignment.revokedTasks());
        if (assignmentSnapshot != null) {
            assignmentSnapshot.connectors().removeAll(newAssignment.revokedConnectors());
            assignmentSnapshot.tasks().removeAll(newAssignment.revokedTasks());
            newAssignment.connectors().addAll(assignmentSnapshot.connectors());
            newAssignment.tasks().addAll(assignmentSnapshot.tasks());
        }
        assignmentSnapshot = newAssignment;
        listener.onAssigned(assignmentSnapshot, generation);
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId, String protocol, List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
        if (protocolCompatibility == COOP) {
            return assignor.performAssignment(leaderId, protocol, allMemberMetadata,
                    configSnapshot, configStorage, leaderState);
        }
        log.debug("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseData.JoinGroupResponseMember memberMetadata : allMemberMetadata)
            memberConfigs.put(memberMetadata.memberId(), IncrementalCooperativeConnectProtocol.deserializeMetadata(ByteBuffer.wrap(memberMetadata.metadata())));

        long maxOffset = findMaxMemberConfigOffset(memberConfigs);
        Long leaderOffset = ensureLeaderConfig(maxOffset);
        if (leaderOffset == null)
            return fillAssignmentsAndSerialize(memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    new HashMap<>(), new HashMap<>());
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs);
    }

    private long findMaxMemberConfigOffset(Map<String, ExtendedWorkerState> memberConfigs) {
        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        Long maxOffset = null;
        for (Map.Entry<String, ExtendedWorkerState> stateEntry : memberConfigs.entrySet()) {
            long memberRootOffset = stateEntry.getValue().offset();
            if (maxOffset == null)
                maxOffset = memberRootOffset;
            else
                maxOffset = Math.max(maxOffset, memberRootOffset);
        }

        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, configSnapshot.get().offset());
        return maxOffset;
    }

    private Long ensureLeaderConfig(long maxOffset) {
        // If this leader is behind some other members, we can't do assignment
        if (configSnapshot.get().offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = configStorage.snapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. " +
                        "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                configSnapshot.set(updatedSnapshot);
                return configSnapshot.get().offset();
            }
        }

        return maxOffset;
    }

    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset, Map<String, ExtendedWorkerState> memberConfigs) {
        Map<String, Collection<String>> connectorAssignments = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> taskAssignments = new HashMap<>();

        // Perform round-robin task assignment. Assign all connectors and then all tasks because assigning both the
        // connector and its tasks can lead to very uneven distribution of work in some common cases (e.g. for connectors
        // that generate only 1 task each; in a cluster of 2 or an even # of nodes, only even nodes will be assigned
        // connectors and only odd nodes will be assigned tasks, but tasks are, on average, actually more resource
        // intensive than connectors).
        List<String> connectorsSorted = sorted(configSnapshot.get().connectors());
        CircularIterator<String> memberIt = new CircularIterator<>(sorted(memberConfigs.keySet()));
        for (String connectorId : connectorsSorted) {
            String connectorAssignedTo = memberIt.next();
            log.trace("Assigning connector {} to {}", connectorId, connectorAssignedTo);
            Collection<String> memberConnectors = connectorAssignments.get(connectorAssignedTo);
            if (memberConnectors == null) {
                memberConnectors = new ArrayList<>();
                connectorAssignments.put(connectorAssignedTo, memberConnectors);
            }
            memberConnectors.add(connectorId);
        }
        for (String connectorId : connectorsSorted) {
            for (ConnectorTaskId taskId : sorted(configSnapshot.get().tasks(connectorId))) {
                String taskAssignedTo = memberIt.next();
                log.trace("Assigning task {} to {}", taskId, taskAssignedTo);
                Collection<ConnectorTaskId> memberTasks = taskAssignments.get(taskAssignedTo);
                if (memberTasks == null) {
                    memberTasks = new ArrayList<>();
                    taskAssignments.put(taskAssignedTo, memberTasks);
                }
                memberTasks.add(taskId);
            }
        }

        leaderState.set(new LeaderState(memberConfigs, connectorAssignments, taskAssignments));

        return fillAssignmentsAndSerialize(memberConfigs.keySet(), Assignment.NO_ERROR,
                leaderId, memberConfigs.get(leaderId).url(), maxOffset, connectorAssignments, taskAssignments);
    }

    private Map<String, ByteBuffer> fillAssignmentsAndSerialize(Collection<String> members,
                                                                short error,
                                                                String leaderId,
                                                                String leaderUrl,
                                                                long maxOffset,
                                                                Map<String, Collection<String>> connectorAssignments,
                                                                Map<String, Collection<ConnectorTaskId>> taskAssignments) {

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectors = connectorAssignments.get(member);
            if (connectors == null)
                connectors = Collections.emptyList();
            Collection<ConnectorTaskId> tasks = taskAssignments.get(member);
            if (tasks == null)
                tasks = Collections.emptyList();
            ByteBuffer serializedAssignment;
            if (protocolCompatibility == COOP) {
                ExtendedAssignment assignment = new ExtendedAssignment(
                        error, leaderId, leaderUrl, maxOffset, connectors, tasks,
                        Collections.emptyList(), Collections.emptyList());
                log.debug("Assignment: {} -> {}", member, assignment);
                serializedAssignment = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment);
            } else {
                Assignment assignment = new Assignment(error, leaderId, leaderUrl, maxOffset, connectors, tasks);
                log.debug("Assignment: {} -> {}", member, assignment);
                serializedAssignment = ConnectProtocol.serializeAssignment(assignment);
            }
            groupAssignment.put(member, serializedAssignment);
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        log.info("Rebalance started");
        leaderState.set(null);
        if (protocolCompatibility == COOP) {
            log.debug("Cooperative rebalance triggered. Keeping assignment {} until it's "
                      + "explicitly revoked.", assignmentSnapshot);
        } else {
            log.debug("Revoking previous assignment {}", assignmentSnapshot);
            if (assignmentSnapshot != null && !assignmentSnapshot.failed())
                listener.onRevoked(assignmentSnapshot.leader(), assignmentSnapshot.connectors(), assignmentSnapshot.tasks());
        }
    }

    @Override
    protected boolean rejoinNeededOrPending() {
        return super.rejoinNeededOrPending() || (assignmentSnapshot == null || assignmentSnapshot.failed()) || rejoinRequested;
    }

    public String memberId() {
        Generation generation = generation();
        if (generation != null)
            return generation.memberId;
        return JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    private boolean isLeader() {
        return assignmentSnapshot != null && memberId().equals(assignmentSnapshot.leader());
    }

    public String ownerUrl(String connector) {
        if (rejoinNeededOrPending() || !isLeader())
            return null;
        return leaderState.get().ownerUrl(connector);
    }

    public String ownerUrl(ConnectorTaskId task) {
        if (rejoinNeededOrPending() || !isLeader())
            return null;
        return leaderState.get().ownerUrl(task);
    }

    private class WorkerCoordinatorMetrics {
        public final String metricGrpName;

        public WorkerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            Measurable numConnectors = new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.connectors().size();
                }
            };

            Measurable numTasks = new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.tasks().size();
                }
            };

            metrics.addMetric(metrics.metricName("assigned-connectors",
                              this.metricGrpName,
                              "The number of connector instances currently assigned to this consumer"), numConnectors);
            metrics.addMetric(metrics.metricName("assigned-tasks",
                              this.metricGrpName,
                              "The number of tasks currently assigned to this consumer"), numTasks);
        }
    }

    private static <T extends Comparable<T>> List<T> sorted(Collection<T> members) {
        List<T> res = new ArrayList<>(members);
        Collections.sort(res);
        return res;
    }

    private static <K, V> Map<V, K> invertAssignment(Map<K, Collection<V>> assignment) {
        Map<V, K> inverted = new HashMap<>();
        for (Map.Entry<K, Collection<V>> assignmentEntry : assignment.entrySet()) {
            K key = assignmentEntry.getKey();
            for (V value : assignmentEntry.getValue())
                inverted.put(value, key);
        }
        return inverted;
    }

    public static class LeaderState {
        private final Map<String, ExtendedWorkerState> allMembers;
        private final Map<String, String> connectorOwners;
        private final Map<ConnectorTaskId, String> taskOwners;

        public LeaderState(Map<String, ExtendedWorkerState> allMembers,
                           Map<String, Collection<String>> connectorAssignment,
                           Map<String, Collection<ConnectorTaskId>> taskAssignment) {
            this.allMembers = allMembers;
            this.connectorOwners = invertAssignment(connectorAssignment);
            this.taskOwners = invertAssignment(taskAssignment);
        }

        private String ownerUrl(ConnectorTaskId id) {
            String ownerId = taskOwners.get(id);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

        private String ownerUrl(String connector) {
            String ownerId = connectorOwners.get(connector);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

    }

    public static class ConnectorsAndTasks {
        private final Collection<String> connectors;
        private final Collection<ConnectorTaskId> tasks;
        public static final ConnectorsAndTasks EMPTY =
                new ConnectorsAndTasks(Collections.emptyList(), Collections.emptyList());

        private ConnectorsAndTasks(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            this.connectors = connectors;
            this.tasks = tasks;
        }

        public static ConnectorsAndTasks copy(Collection<String> connectors,
                                              Collection<ConnectorTaskId> tasks) {
            return new ConnectorsAndTasks(new ArrayList<>(connectors), new ArrayList<>(tasks));
        }

        public static ConnectorsAndTasks embed(Collection<String> connectors,
                                               Collection<ConnectorTaskId> tasks) {
            return new ConnectorsAndTasks(connectors, tasks);
        }

        public Collection<String> connectors() {
            return connectors;
        }

        public Collection<ConnectorTaskId> tasks() {
            return tasks;
        }

        public int size() {
            return connectors.size() + tasks.size();
        }

        public boolean isEmpty() {
            return connectors.isEmpty() && tasks.isEmpty();
        }

        @Override
        public String toString() {
            return "{ connectorIds=" + connectors + ", taskIds=" + tasks + '}';
        }
    }


    public static class WorkerLoad {
        private String worker;
        private final Collection<String> connectors;
        private final Collection<ConnectorTaskId> tasks;

        private WorkerLoad(
                String worker,
                Collection<String> connectors,
                Collection<ConnectorTaskId> tasks
        ) {
            this.worker = worker;
            this.connectors = connectors;
            this.tasks = tasks;
        }

        public static WorkerLoad copy(
                String worker,
                Collection<String> connectors,
                Collection<ConnectorTaskId> tasks
        ) {
            return new WorkerLoad(worker, new ArrayList<>(connectors), new ArrayList<>(tasks));
        }

        public static WorkerLoad embed(
                String worker,
                Collection<String> connectors,
                Collection<ConnectorTaskId> tasks
        ) {
            return new WorkerLoad(worker, connectors, tasks);
        }

        public String worker() {
            return worker;
        }

        public Collection<String> connectors() {
            return connectors;
        }

        public Collection<ConnectorTaskId> tasks() {
            return tasks;
        }

        public int connectorsSize() {
            return connectors.size();
        }

        public int tasksSize() {
            return tasks.size();
        }

        public void assign(String connector) {
            connectors.add(connector);
        }

        public void assign(ConnectorTaskId task) {
            tasks.add(task);
        }

        public int size() {
            return connectors.size() + tasks.size();
        }

        public boolean isEmpty() {
            return connectors.isEmpty() && tasks.isEmpty();
        }

        public static Comparator<WorkerLoad> connectorComparator() {
            return (left, right) -> {
                int res = left.connectors.size() - right.connectors.size();
                return res != 0 ? res : left.worker == null
                                        ? right.worker == null ? 0 : -1
                                        : left.worker.compareTo(right.worker);
            };
        }

        public static Comparator<WorkerLoad> taskComparator() {
            return (left, right) -> {
                int res = left.tasks.size() - right.tasks.size();
                return res != 0 ? res : left.worker == null
                                        ? right.worker == null ? 0 : -1
                                        : left.worker.compareTo(right.worker);
            };
        }

        @Override
        public String toString() {
            return "{ connectorIds=" + connectors + ", taskIds=" + tasks + '}';
        }
    }

}
