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