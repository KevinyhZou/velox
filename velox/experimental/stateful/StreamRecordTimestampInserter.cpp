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
#include "velox/experimental/stateful/StreamRecordTimestampInserter.h"

#include "velox/experimental/stateful/WatermarkGenerator.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful {

StreamRecordTimestampInserter::StreamRecordTimestampInserter(
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    int rowtimeFieldIndex)
    : StatefulOperator(std::move(op), std::move(targets)),
      rowtimeFieldIndex_(rowtimeFieldIndex) {}

void StreamRecordTimestampInserter::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  input_ = record->record();
  op()->addInput(input_);
}

void StreamRecordTimestampInserter::advance() {
  if (!input_) {
    return;
  }
  watermark::validateRowtimeNoNulls(input_, rowtimeFieldIndex_);
  // The wrapped FilterProject projects the rowtime column into a single-column
  // RowVector; drain it to read timestamps.
  RowVectorPtr timestampVector =
      watermark::getTimestampVector(op().get(), input_);

  VELOX_CHECK_EQ(
      timestampVector->childrenSize(),
      1,
      "Expected single-column timestamp vector from wrapped FilterProject");
  auto child = timestampVector->childAt(0);
  auto tsVector = child->as<SimpleVector<Timestamp>>();
  VELOX_CHECK_NOT_NULL(
      tsVector, "Rowtime column is not a SimpleVector<Timestamp>");
  const vector_size_t timestampSize = timestampVector->size();
  if (timestampSize == 0) {
    pushOutput(
        std::make_shared<StreamRecord>(getPlanNodeId(), std::move(input_)));
    input_.reset();
    return;
  }
  int64_t maxTs = std::numeric_limits<int64_t>::min();
  for (vector_size_t i = 0; i < timestampSize; ++i) {
    const int64_t millis = tsVector->valueAt(i).toMillis();
    if (millis > maxTs) {
      maxTs = millis;
    }
  }

  pushOutput(std::make_shared<StreamRecord>(
      getPlanNodeId(), std::move(input_), maxTs));
  input_.reset();
}

void StreamRecordTimestampInserter::close() {
  StatefulOperator::close();
  input_.reset();
}

} // namespace facebook::velox::stateful
