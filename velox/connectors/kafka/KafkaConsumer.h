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

#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include "velox/connectors/kafka/KafkaRecordDeserializer.h"
#include "cppkafka/cppkafka.h"
#include "cppkafka/topic_partition_list.h"
#include "folly/ProducerConsumerQueue.h"

namespace facebook::velox::connector::kafka {

using String = std::string;
using MessageQueuePtr = std::shared_ptr<folly::ProducerConsumerQueue<String>>;
using CppKafkaConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class KafkaConsumer {
public:
    KafkaConsumer(
        const CppKafkaConsumerPtr & consumer,
        const uint32_t pollTimeOut,
        const uint32_t pollBatchSize) 
    : consumer_(consumer),
      pollTimeOutMillis_(pollTimeOut),
      pollBatchSize_(pollBatchSize) {}
    
    ~KafkaConsumer() {
        running_ = false;
    }

    const cppkafka::TopicPartitionList getTopicPartitions(const String & topic, const String & startupMode);

    void subscribe(const std::vector<String> & topics);

    void assign(const cppkafka::TopicPartitionList & tps);

    void stop();

    const void consumeBatch(std::vector<String> & msgs, size_t & msg_bytes);

    const void consumeBatch(std::vector<cppkafka::Message> & msgs);

private:
    CppKafkaConsumerPtr consumer_;
    bool running_ = false;
    std::chrono::milliseconds pollTimeOutMillis_;
    uint32_t pollBatchSize_;
};

using KafkaConsumerPtr = std::shared_ptr<KafkaConsumer>;
}