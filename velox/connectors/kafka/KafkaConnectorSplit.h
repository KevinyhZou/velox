/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/connectors/Connector.h"
#include "cppkafka/topic_partition_list.h"
#include "cppkafka/topic_partition.h"
#include "folly/dynamic.h"

namespace facebook::velox::connector::kafka {

using String = std::string;

struct KafkaConnectorSplit : public ConnectorSplit {
    String bootstrapServers_;
    String groupId_;
    String clientId_;
    String format_;
    bool enableAutoCommit_;
    String autoResetOffset_;
    std::unordered_map<String, std::vector<std::pair<uint32_t, int64_t>>> topicPartitions_;
    
    explicit KafkaConnectorSplit(
        const String & connectorId,
        const String & bootstrapServers,
        const String & groupId,
        const String & format,
        const bool enableAutoCommit,
        const String & autoResetOffset,
        std::unordered_map<String, std::vector<std::pair<uint32_t, int64_t>>> & tps)
        : ConnectorSplit(connectorId),
        bootstrapServers_(bootstrapServers),
        groupId_(groupId),
        clientId_(connectorId), 
        format_(format), 
        enableAutoCommit_(enableAutoCommit),
        autoResetOffset_(autoResetOffset),
        topicPartitions_(tps) {}

    cppkafka::TopicPartitionList getCppKafkaTopicPartitions() const {
        cppkafka::TopicPartitionList topicPartitions;
        for (const auto & p : topicPartitions_) {
            String topic = p.first;
            for (const auto & partition : p.second) {
                cppkafka::TopicPartition topicPartition(topic, static_cast<int>(partition.first));
                if (partition.second >= 0) {
                    topicPartition.set_offset(partition.second);
                }
                topicPartitions.emplace_back(topicPartition);
            }
        }
        return topicPartitions;
    }

    static String topicPartitonsToString(const cppkafka::TopicPartitionList & tps);

    String toString() const override;

    folly::dynamic serialize() const override;

    static std::shared_ptr<KafkaConnectorSplit> create(const folly::dynamic& obj);

    static void registerSerDe();
};
}