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

#include "velox/type/Type.h"
#include "velox/type/Filter.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/kafka/KafkaConfig.h"
#include "velox/connectors/kafka/KafkaConnectorSplit.h"
#include "velox/connectors/kafka/KafkaConsumer.h"
#include "velox/connectors/kafka/KafkaRecordDeserializer.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "folly/Executor.h"
#include "folly/ProducerConsumerQueue.h"
#include "cppkafka/cppkafka.h"

namespace facebook::velox::connector::kafka {

using TableHandlePtr = std::shared_ptr<connector::ConnectorTableHandle>;
using ConnectorSplitPtr = std::shared_ptr<ConnectorSplit>;
using NonConstColumnHandlePtr = std::shared_ptr<connector::ColumnHandle>;

class KafkaDataSource : public DataSource {
 public:
   KafkaDataSource(
      const RowTypePtr & outputType,
      const TableHandlePtr & tableHandle,
      const std::unordered_map<std::string, NonConstColumnHandlePtr> & columnHandles,
      folly::Executor* executor,
      const ConnectorQueryCtx* connectorQueryCtx,
      const ConnectionConfigPtr & connectionConfig);

  /// Create a kafka connection to the given topics and partitions.
  void addSplit(ConnectorSplitPtr split) override;

  /// Fetch record from the consumed records.
  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future) override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  void cancel() override;

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

private:
  ConnectionConfigPtr config_;
  RowTypePtr outputType_;
  std::vector<String> topics_; 
  std::shared_ptr<folly::Executor> executor_;
  const ConnectorQueryCtx* queryCtx_;
  KafkaConsumerPtr consumer_;
  KafkaRecordDeserializerPtr deserializer_;
  MessageQueuePtr queue_;
  uint64_t completedRows_ = 0;
  uint64_t completedBytes_ = 0;

  /// consumer can be created.
  bool consumerCanbeCreated();

  /// create kafka consumer from the configuration.
  void createConsumer(cppkafka::Configuration & config);

  /// create message queue with given size.
  void createMessageQueue(const uint32_t size);

  /// create deserializer to deserialize the consumed recored to the given row type.
  void createRecordDeserializer(const String & format, const RowTypePtr & outputType);
};

} // namespace facebook::velox::connector::kafka

