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

#include "velox/connectors/kafka/KafkaDataSource.h"
#include "velox/connectors/kafka/KafkaTableHandle.h"
#include "velox/connectors/kafka/format/CSVRecordDeserializer.h"
#include "velox/connectors/kafka/format/JSONRecordDeserializer.h"
#include "velox/connectors/kafka/format/RawRecordDeserializer.h"
#include "velox/connectors/kafka/format/StreamJSONRecordDeserializer.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/BaseVector.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include <iostream>

namespace facebook::velox::connector::kafka {
    
    KafkaDataSource::KafkaDataSource(const RowTypePtr & outputType,
      const TableHandlePtr & tableHandle,
      const std::unordered_map<std::string, NonConstColumnHandlePtr> & columnHandles,
      folly::Executor*,
      const ConnectorQueryCtx* connectorQueryCtx,
      const ConnectionConfigPtr & config) :
        queryCtx_(connectorQueryCtx),
        config_(config),
        outputType_(outputType),
        processByBatch_(config_->getEnableBatchProcessData()),
        consumeBatchSize_(config_->getPollMaxBatchSize()) {
        const std::shared_ptr<KafkaTableHandle> kafkaTableHandle = std::dynamic_pointer_cast<KafkaTableHandle>(tableHandle);
        if (kafkaTableHandle) {
            const std::unordered_map<String, String> & tableParams = kafkaTableHandle->tableParameters();
            config_ = config_->setConfigs<ConnectionConfig>(tableParams);
            if (kafkaTableHandle->projectedDataColumns()) {
                outputType_ = kafkaTableHandle->projectedDataColumns();
            }
        } else {
            VELOX_FAIL("The table handle {} is not supported for kafka data source.", tableHandle->connectorId());
        }
        if (consumerCanbeCreated()) {
            cppkafka::Configuration cppKafkaConfig = config_->getCppKafkaConfiguration();
            createConsumer(cppKafkaConfig);
        }
        createMessageQueue(consumeBatchSize_);
        createRecordDeserializer(config_->getFormat(), outputType_);
    }

    bool KafkaDataSource::consumerCanbeCreated() {
        return config_->exists(ConnectionConfig::kBootstrapServers)
            && config_->exists(ConnectionConfig::kClientId)
            && config_->exists(ConnectionConfig::kTopic)
            && config_->exists(ConnectionConfig::kGroupId)
            && config_->exists(ConnectionConfig::kFormat)
            && !consumer_.get();
    }

    void KafkaDataSource::createConsumer(cppkafka::Configuration & config) {
        VELOX_CHECK_NULL(consumer_.get(), "Failed to create kafka consumer as the consumer is not null");
        CppKafkaConsumerPtr cppKafkaConsumer = std::make_shared<cppkafka::Consumer>(config);
        cppKafkaConsumer->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
        consumer_ = std::make_shared<KafkaConsumer>(cppKafkaConsumer, config_->getPollTimeoutMills(), consumeBatchSize_);
        String topic = config_->getTopic();
        topics_.emplace_back(topic);
        consumer_->subscribe(topics_);
    }

    void KafkaDataSource::createMessageQueue(const uint32_t size) {
        VELOX_CHECK_GT(size, 0, "Kafka consume message queue size must greater than 0");
        queue_.reserve(size);
    }

    void KafkaDataSource::createRecordDeserializer(const String & format, const RowTypePtr & outputType) {
        if (format == "json") {
            deserializer_ = std::make_shared<KafkaJSONRecordDeserializer>(outputType, queryCtx_->memoryPool()); 
        } else if (format == "csv") {
            deserializer_ = std::make_shared<KafkaCSVRecordDeserializer>(outputType, queryCtx_->memoryPool());
        } else if (format == "raw") {
            deserializer_ = std::make_shared<KafkaRawRecordDeserializer>(outputType, queryCtx_->memoryPool());
        } else {
            VELOX_FAIL_UNSUPPORTED_INPUT_UNCATCHABLE("The data format {} is not supported for kafka.", format);
        }
        emptyRow_ = deserializer_->emptyRow();
        outRow_ = deserializer_->emptyRow();
        outRow_->resize(1);
    }

    void KafkaDataSource::addSplit(ConnectorSplitPtr split) {
        KafkaConnectorSplit * kafkaConnectorSplit = static_cast<KafkaConnectorSplit *>(split.get());
        VELOX_CHECK_NOT_NULL(kafkaConnectorSplit, "Failed to add split, because the kafka connector split is null.");
        VELOX_CHECK_NOT_NULL(consumer_.get(), "Failed to add split, because the kafka consumer is null.");
        cppkafka::TopicPartitionList topicPartitions = kafkaConnectorSplit->getCppKafkaTopicPartitions();
        consumer_->assign(topicPartitions);
    }

    std::optional<RowVectorPtr> KafkaDataSource::next(uint64_t, velox::ContinueFuture&) {
        std::optional<RowVectorPtr> res;
        if (processByBatch_) {
            size_t msgBytes = 0;
            std::vector<String> msgs;
            consumer_->consumeBatch(msgs, msgBytes);
            if (msgs.empty()) {
                res.emplace(emptyRow_);
            } else {
                completedRows_ += msgs.size();
                completedBytes_ += msgBytes;
                outRow_->prepareForReuse();
                outRow_->resize(msgs.size());
                deserializer_->deserialize(msgs, outRow_);
                res.emplace(std::dynamic_pointer_cast<RowVector>(outRow_));
            }
        } else {
            if (consumePos_ == queue_.size()) {
                consumer_->consumeBatch(queue_);
                consumePos_ = 0;
                res.emplace(emptyRow_);
            } else {
                outRow_->prepareForReuse();
                deserializer_->deserialize(queue_[consumePos_].get_payload(), 0, outRow_);
                completedRows_ += 1;
                completedBytes_ += queue_[consumePos_].get_payload().get_size();
                res.emplace(std::dynamic_pointer_cast<RowVector>(outRow_));
                consumePos_ ++;
            }
        }
        return res;
    }

    std::unordered_map<String, RuntimeCounter> KafkaDataSource::runtimeStats() {
        std::unordered_map<String, RuntimeCounter> stats;
        return stats;
    }

    void KafkaDataSource::cancel() {
        if (consumer_) {
            consumer_->stop();
        }
    }
}
