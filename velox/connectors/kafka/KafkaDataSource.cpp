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
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"
#include "folly/executors/CPUThreadPoolExecutor.h"

namespace facebook::velox::connector::kafka {
    
    KafkaDataSource::KafkaDataSource(const RowTypePtr & outputType,
      const TableHandlePtr & tableHandle,
      const std::unordered_map<std::string, NonConstColumnHandlePtr> & columnHandles,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const ConnectionConfigPtr & config) :
        config_(config),
        outputType_(outputType),
        executor_(std::shared_ptr<folly::Executor>(executor)),
        queryCtx_(connectorQueryCtx) {
        const std::shared_ptr<KafkaTableHandle> kafkaTableHandle = std::dynamic_pointer_cast<KafkaTableHandle>(tableHandle);
        if (kafkaTableHandle) {
            const std::unordered_map<String, String> & tableParams = kafkaTableHandle->tableParameters();
            config_ = config_->setConfigs<ConnectionConfig>(tableParams);
        } else {
            VELOX_FAIL("The table handle {} is not supported for kafka data source.", tableHandle->connectorId());
        }
        if (!executor_) {
            executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
        }
        createMessageQueue(config_->getConsumeQueueSize());
        if (consumerCanbeCreated()) {
            cppkafka::Configuration cppKafkaConfig = config_->getCppKafkaConfiguration();
            createConsumer(cppKafkaConfig);
        }
        createRecordDeserializer(config_->getFormat(), outputType);
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
        VELOX_CHECK_NOT_NULL(queue_.get(), "Failed to create kafka consumer as the message queue is null");
        VELOX_CHECK_NOT_NULL(executor_, "Failed to create kafka consumer as the executor is null");
        consumer_ = std::make_shared<KafkaConsumer>(cppKafkaConsumer, queue_, 
            config_->getPollTimeoutMills(), config_->getPollMaxBatchSize(), executor_);
        String topic = config_->getTopic();
        topics_.emplace_back(topic);
        consumer_->subscribe(topics_);
    }

    void KafkaDataSource::createMessageQueue(const uint32_t size) {
        VELOX_CHECK_GT(size, 0, "Kafka consume message queue size must greater than 0");
        queue_ = std::make_shared<folly::ProducerConsumerQueue<String>>(size);
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
    }

    void KafkaDataSource::addSplit(ConnectorSplitPtr split) {
        KafkaConnectorSplit * kafkaConnectorSplit = static_cast<KafkaConnectorSplit *>(split.get());
        VELOX_CHECK_NOT_NULL(kafkaConnectorSplit, "Failed to add split, because the kafka connector split is null.");
        VELOX_CHECK_NOT_NULL(consumer_.get(), "Failed to add split, because the kafka consumer is null.");
        cppkafka::TopicPartitionList topicPartitions = kafkaConnectorSplit->getCppKafkaTopicPartitions();
        consumer_->assign(topicPartitions);
    }

    std::optional<RowVectorPtr> KafkaDataSource::next(uint64_t, velox::ContinueFuture&) {
        std::optional<RowVectorPtr> row;
        String msg;
        if (queue_->read(msg)) {
            VELOX_CHECK_NOT_NULL(deserializer_.get(), "Failed to deserialize the message, because the deserializer is null.");
            const RowVectorPtr vec = deserializer_->deserialize(msg);
            row.emplace(vec);
            completedRows_ += 1;
            completedBytes_ += msg.size();
        } else {
            const RowVectorPtr vec = deserializer_->emptyRow();
            row.emplace(vec);
        }
        return row;
    }

    std::unordered_map<String, RuntimeCounter> KafkaDataSource::runtimeStats() {
        std::unordered_map<String, RuntimeCounter> stats;
        return stats;
    }

    void KafkaDataSource::cancel() {
        if (consumer_) {
            consumer_->stop();
        }
        if (executor_) {
            std::shared_ptr<folly::CPUThreadPoolExecutor> cpuExecutor =
                std::dynamic_pointer_cast<folly::CPUThreadPoolExecutor>(executor_);
            cpuExecutor->stop();
        }
    }
}
