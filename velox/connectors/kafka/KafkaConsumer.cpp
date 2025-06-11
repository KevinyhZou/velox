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

#include "velox/connectors/kafka/KafkaConsumer.h"
#include "cppkafka/buffer.h"
#include "cppkafka/consumer.h"
#include <iostream>

namespace facebook::velox::connector::kafka {

    void KafkaConsumer::subscribe(const std::vector<String> & topics) {
        auto topicsToString = [&]() -> String {
            String s = "";
            if (topics.empty()) {
                return s;
            }
            for (size_t i = 0; i < topics.size() - 1; ++i) {
                s += topics[i];
                s += ",";
            }
            s += topics[topics.size() - 1];
            return s;
        };
        VELOX_CHECK_NOT_NULL(consumer_.get(), "Failed to subscribe to topics: {}, as the cppkafka consumer is null.", topicsToString());
        consumer_->subscribe(topics);
    }

    const cppkafka::TopicPartitionList KafkaConsumer::getTopicPartitions(const String & topic, const String & startupMode) {
        cppkafka::TopicPartitionList tps;
        auto metadata = consumer_->get_metadata();
        const auto & topics = metadata.get_topics();
        for (const cppkafka::TopicMetadata & topicMetadata : topics) {
            if (topicMetadata.get_name() == topic) {
                const auto & partitions = topicMetadata.get_partitions();
                for (const auto & partition : partitions) {
                    cppkafka::TopicPartition topicPartition(topic, static_cast<int>(partition.get_id()));
                    auto offsets = consumer_->query_offsets(topicPartition);
                    if (startupMode == "earliest-offsets") {
                        topicPartition.set_offset(std::get<0>(offsets));
                    } else if (startupMode == "latest-offsets") {
                        topicPartition.set_offset(std::get<1>(offsets));
                    }
                    tps.emplace_back(topicPartition);
                }
            }
        }
        if (tps.size() == 0) {
            VELOX_FAIL("Failed to find topic {}", topic);
        }
        return tps;
    }

    void KafkaConsumer::assign(const cppkafka::TopicPartitionList & tps) {
        String tpsString = KafkaConnectorSplit::topicPartitonsToString(tps);
        VELOX_CHECK_NOT_NULL(consumer_.get(), "Failed to assign topic partitions: {}, as the cppkafka consumer is null.", tpsString);
        consumer_->assign(tps);
        running_ = true;
    }

    const void KafkaConsumer::consumeBatch(std::vector<String> & res, size_t & msg_bytes) {
        const std::vector<cppkafka::Message> msgs = consumer_->poll_batch(pollBatchSize_);
        for (const auto & msg : msgs) {
            const String & msgData = msg.get_payload();
            msg_bytes += msgData.size();
            res.emplace_back(msgData);
        }
    }

    const void KafkaConsumer::consumeBatch(std::vector<cppkafka::Message> & msgs) {
        msgs.clear();
        msgs = consumer_->poll_batch(pollBatchSize_);
    }

    void KafkaConsumer::stop() {
        running_ = false;
    }
}