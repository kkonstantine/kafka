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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;

/**
 * Utility to simplify creating and managing topics via the {@link Admin}.
 */
public class TopicCreationGroup {
    private final String name;
    private final Pattern inclusionPattern;
    private final Pattern exclusionPattern;
    private final int numPartitions;
    private final short replicationFactor;
    private final Map<String, Object> otherConfigs;

    protected TopicCreationGroup(String group, SourceConnectorConfig config) {
        this.name = group;
        this.inclusionPattern = Pattern.compile(String.join(
                "|",
                config.topicCreationInclude(group)
        ));
        this.exclusionPattern = Pattern.compile(String.join(
                "|",
                config.topicCreationExclude(group)
        ));
        this.numPartitions = config.topicCreationPartitions(group);
        this.replicationFactor = config.topicCreationReplicationFactor(group);
        this.otherConfigs = config.topicCreationOtherConfigs(group);
    }

    public static Map<String, TopicCreationGroup> configuredGroups(SourceConnectorConfig config) {
        List<String> groupNames = config.getList(TOPIC_CREATION_GROUPS_CONFIG);
        Map<String, TopicCreationGroup> groups = new LinkedHashMap<>();
        for (String group : groupNames) {
            groups.put(group, new TopicCreationGroup(group, config));
        }
        // Even if there was a group called 'default' in the config, it will be overriden here.
        // Order matters for all the topic groups besides the default, since it will be
        // removed from this collection by the Worker
        groups.put(
                DEFAULT_TOPIC_CREATION_GROUP,
                new TopicCreationGroup(DEFAULT_TOPIC_CREATION_GROUP, config)
        );
        return groups;
    }

    public String name() {
        return name;
    }

    public boolean matches(String topic) {
        return !exclusionPattern.matcher(topic).matches() && inclusionPattern.matcher(topic)
                .matches();
    }

    public NewTopic newTopic(String topic) {
        TopicAdmin.NewTopicBuilder builder = new TopicAdmin.NewTopicBuilder(topic);
        return builder.partitions(numPartitions)
                .replicationFactor(replicationFactor)
                .config(otherConfigs)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicCreationGroup)) {
            return false;
        }
        TopicCreationGroup that = (TopicCreationGroup) o;
        return Objects.equals(name, that.name)
                && numPartitions == that.numPartitions
                && replicationFactor == that.replicationFactor
                && Objects.equals(inclusionPattern.pattern(), that.inclusionPattern.pattern())
                && Objects.equals(exclusionPattern.pattern(), that.exclusionPattern.pattern())
                && Objects.equals(otherConfigs, that.otherConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, numPartitions, replicationFactor, inclusionPattern.pattern(),
                exclusionPattern.pattern(), otherConfigs
        );
    }
}
