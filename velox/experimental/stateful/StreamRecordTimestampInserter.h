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

#include <cstdint>

#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"

namespace facebook::velox::stateful {

/// Native counterpart of Flink's
/// org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter.
/// Reads the rowtime column of each input batch, takes the max timestamp, and
/// emits the original RowVector wrapped in a StreamRecord carrying that
/// timestamp.
class StreamRecordTimestampInserter : public StatefulOperator {
 public:
  StreamRecordTimestampInserter(
      std::unique_ptr<exec::Operator> op,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      int rowtimeFieldIndex);

  void addInput(StreamElementPtr input) override;

  void advance() override;

  std::string name() const override {
    return "StreamRecordTimestampInserter";
  }

  void close() override;

 private:
  RowVectorPtr input_;
  const int rowtimeFieldIndex_;
};

} // namespace facebook::velox::stateful
