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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedAssignment;
import org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedWorkerState;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for managing assignments
 * to workers.
 */
public class ConnectAssignor {
    private final Logger log;
    private ConnectorsAndTasks previousAssignment = ConnectorsAndTasks.EMPTY;

    public ConnectAssignor(LogContext logContext) {
        this.log = logContext.logger(ConnectAssignor.class);
    }

    public Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                                     Map<String, ByteBuffer> allMemberMetadata,
                                                     AtomicReference<ClusterConfigState> configSnapshot,
                                                     ConfigBackingStore configStorage,
                                                     AtomicReference<LeaderState> leaderState) {
        log.debug("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> entry : allMemberMetadata.entrySet())
            memberConfigs.put(entry.getKey(), IncrementalCooperativeConnectProtocol.deserializeMetadata(entry.getValue()));

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                maxOffset, configSnapshot.get().offset());

        Long leaderOffset = ensureLeaderConfig(maxOffset, configSnapshot, configStorage);
        if (leaderOffset == null) {
            Map<String, ExtendedAssignment> assignments =
                    fillAssignments(memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset, Collections.emptyMap(),
                    Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
            return serializeAssignments(assignments);
        }
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs, configSnapshot, leaderState);
    }

    private Long ensureLeaderConfig(long maxOffset,
                                    AtomicReference<ClusterConfigState> configSnapshot,
                                    ConfigBackingStore configStorage) {
        // If this leader is behind some other members, we can't do assignment
        if (configSnapshot.get().offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = configStorage.snapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. "
                         + "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                configSnapshot.set(updatedSnapshot);
                return updatedSnapshot.offset();
            }
        }
        return maxOffset;
    }

    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset,
                                                          Map<String, ExtendedWorkerState> memberConfigs,
                                                          AtomicReference<ClusterConfigState> configSnapshot,
                                                          AtomicReference<LeaderState> leaderState) {
        ConnectorsAndTasks activeAssignments = assignment(memberConfigs);
        log.debug("Active assignments: {}", activeAssignments);
        ConnectorsAndTasks lostAssignments = diff(previousAssignment, activeAssignments);
        log.debug("Lost assignments: {}", lostAssignments);

        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.get().connectors());
        Set<ConnectorTaskId> configuredTasks = configuredConnectors.stream()
                .flatMap(c -> configSnapshot.get().tasks(c).stream())
                .collect(Collectors.toSet());

        ConnectorsAndTasks configured = ConnectorsAndTasks.embed(configuredConnectors, configuredTasks);
        log.debug("Configured assignments: {}", configured);
        ConnectorsAndTasks newSubmissions = diff(configured, previousAssignment);
        log.debug("New assignments: {}", newSubmissions);

        List<WorkerLoad> completeWorkerAssignment = workerAssignment(memberConfigs, true);
        List<WorkerLoad> incrementalWorkerAssignment = workerAssignment(memberConfigs, false);

        if (!lostAssignments.isEmpty()) {
            log.debug("Found the following connectors and tasks missing from previous assignment: "
                      + lostAssignments);
        } else {
            assignConnectors(completeWorkerAssignment, newSubmissions.connectors(), log);
            assignConnectors(incrementalWorkerAssignment, newSubmissions.connectors(), log);
            assignTasks(completeWorkerAssignment, newSubmissions.tasks(), log);
            assignTasks(incrementalWorkerAssignment, newSubmissions.tasks(), log);
        }

        log.debug("Complete assignments: {}", completeWorkerAssignment);
        log.debug("Incremental assignments: {}", incrementalWorkerAssignment);

        Map<String, Collection<String>> connectorAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));

        Map<String, Collection<ConnectorTaskId>> taskAssignments =
                completeWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));

        Map<String, Collection<String>> incrementalConnectorAssignments =
                incrementalWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));

        Map<String, Collection<ConnectorTaskId>> incrementalTaskAssignments =
                incrementalWorkerAssignment.stream().collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));

        log.debug("Exact Incremental Connector assignments: {}", incrementalConnectorAssignments);
        log.debug("Exact Incremental Task assignments: {}", incrementalTaskAssignments);

        leaderState.set(new LeaderState(memberConfigs, connectorAssignments, taskAssignments));

        Map<String, ExtendedAssignment> assignments =
                fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId,
                                memberConfigs.get(leaderId).url(), maxOffset, incrementalConnectorAssignments,
                                incrementalTaskAssignments, Collections.emptyMap(), Collections.emptyMap());

        previousAssignment = ConnectorsAndTasks.embed(
                connectorAssignments.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                taskAssignments.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));

        log.debug("Actual assignments: {}", assignments);
        return serializeAssignments(assignments);
    }

    private Map<String, ExtendedAssignment> fillAssignments(Collection<String> members, short error,
                                                            String leaderId, String leaderUrl, long maxOffset,
                                                            Map<String, Collection<String>> connectorAssignments,
                                                            Map<String, Collection<ConnectorTaskId>> taskAssignments,
                                                            Map<String, Collection<String>> revokedConnectors,
                                                            Map<String, Collection<ConnectorTaskId>> revokedTasks) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectorsToStart = connectorAssignments.getOrDefault(member, Collections.emptyList());
            Collection<String> connectorsToStop = revokedConnectors.getOrDefault(member, Collections.emptyList());
            Collection<ConnectorTaskId> tasksToStart = taskAssignments.getOrDefault(member, Collections.emptyList());
            Collection<ConnectorTaskId> tasksToStop = revokedTasks.getOrDefault(member, Collections.emptyList());
            ExtendedAssignment assignment =
                    new ExtendedAssignment(error, leaderId, leaderUrl, maxOffset, connectorsToStart,
                                           tasksToStart, connectorsToStop, tasksToStop);
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    private Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments) {
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> IncrementalCooperativeConnectProtocol.serializeAssignment(e.getValue())));
    }

    private static ConnectorsAndTasks diff(ConnectorsAndTasks base, ConnectorsAndTasks toSubtract) {
        Collection<String> connectors = new TreeSet<>(base.connectors());
        connectors.removeAll(toSubtract.connectors());
        Collection<ConnectorTaskId> tasks = new TreeSet<>(base.tasks());
        tasks.removeAll(toSubtract.tasks());
        return ConnectorsAndTasks.embed(connectors, tasks);
    }

    private static ConnectorsAndTasks assignment(Map<String, ExtendedWorkerState> memberConfigs) {
        Set<String> connectors = memberConfigs.values()
                .stream()
                .flatMap(state -> state.assignment().connectors().stream())
                .collect(Collectors.toSet());
        Set<ConnectorTaskId> tasks = memberConfigs.values()
                .stream()
                .flatMap(state -> state.assignment().tasks().stream())
                .collect(Collectors.toSet());
        return ConnectorsAndTasks.embed(connectors, tasks);
    }

    private static void assignConnectors(List<WorkerLoad> workerAssignment,
                                         Collection<String> connectors,
                                         Logger log) {
        workerAssignment.sort(WorkerLoad.connectorComparator());
        WorkerLoad first = workerAssignment.get(0);

        Iterator<String> load = connectors.iterator();
        while (load.hasNext()) {
            int firstLoad = first.connectorsSize();
            int upTo = IntStream.range(0, workerAssignment.size())
                    .filter(i -> workerAssignment.get(i).connectorsSize() > firstLoad)
                    .findFirst()
                    .orElse(workerAssignment.size() - 1) + 1;
            for (WorkerLoad worker : workerAssignment.subList(0, upTo)) {
                String connector = load.next();
                log.debug("Assigning connector {} to {}", connector, worker.worker());
                worker.assign(connector);
                if (!load.hasNext()) {
                    break;
                }
            }
        }
    }

    private static void assignTasks(List<WorkerLoad> workerAssignment,
                                    Collection<ConnectorTaskId> tasks,
                                    Logger log) {
        workerAssignment.sort(WorkerLoad.taskComparator());
        WorkerLoad first = workerAssignment.get(0);

        Iterator<ConnectorTaskId> load = tasks.iterator();
        while (load.hasNext()) {
            int firstLoad = first.tasksSize();
            int upTo = IntStream.range(0, workerAssignment.size())
                    .filter(i -> workerAssignment.get(i).tasksSize() > firstLoad)
                    .findFirst()
                    .orElse(workerAssignment.size() - 1) + 1;
            for (WorkerLoad worker : workerAssignment.subList(0, upTo)) {
                ConnectorTaskId task = load.next();
                log.debug("Assigning task {} to {}", task, worker.worker());
                worker.assign(task);
                if (!load.hasNext()) {
                    break;
                }
            }
        }
    }
    private static List<WorkerLoad> workerAssignment(Map<String, ExtendedWorkerState> memberConfigs, boolean complete) {
        return memberConfigs.entrySet()
                .stream()
                .map(e -> WorkerLoad.copy(
                        e.getKey(),
                        complete ? e.getValue().assignment().connectors() : new ArrayList<>(),
                        complete ? e.getValue().assignment().tasks() : new ArrayList<>()))
                .collect(Collectors.toList());
    }
}
