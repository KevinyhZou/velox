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

#include <memory>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// This class is relevant to Flink RecordCounter
/// (org.apache.flink.table.runtime.operators.aggregate.RecordCounter).
///
/// It is used by the group aggregate operator to determine whether any input
/// records have been aggregated under the current key. When the count is
/// zero, the accumulator is empty and the corresponding state entry can be
/// cleaned up.
class RecordCounter {
 public:
  virtual ~RecordCounter() = default;

  /// We store the counter in the accumulator. If the counter is not zero,
  /// which means we aggregated at least one record for the current key.
  ///
  /// @return true if input record count is zero, false if not.
  virtual bool recordCountIsZero(const RowVectorPtr& acc) const = 0;

  /// Creates a RecordCounter depending on the index of COUNT(*). If the
  /// index is negative the input stream is append-only and a
  /// AccumulationRecordCounter is returned; otherwise a
  /// RetractionRecordCounter is returned.
  ///
  /// @param indexOfCountStar The index of COUNT(*) in the accumulators. -1
  /// when the input doesn't contain COUNT(*), i.e. doesn't contain
  /// retraction messages. The planner makes sure there is a COUNT(*) if the
  /// input stream contains retractions.
  static std::unique_ptr<RecordCounter> of(int indexOfCountStar);
};

/// RecordCounter for append-only input streams. When all the inputs are
/// accumulations, the count will never be zero, so we only need to detect a
/// missing (null) accumulator.
class AccumulationRecordCounter : public RecordCounter {
 public:
  bool recordCountIsZero(const RowVectorPtr& acc) const override {
    return acc == nullptr;
  }
};

/// RecordCounter for input streams that contain retractions. The COUNT(*)
/// column at index 'indexOfCountStar' tracks the live record count; when it
/// reaches zero, the accumulator should be considered empty.
class RetractionRecordCounter : public RecordCounter {
 public:
  explicit RetractionRecordCounter(int indexOfCountStar)
      : indexOfCountStar_(indexOfCountStar) {
    VELOX_CHECK_GE(
        indexOfCountStar_,
        0,
        "RetractionRecordCounter requires a non-negative COUNT(*) index");
  }

  bool recordCountIsZero(const RowVectorPtr& acc) const override {
    if (acc == nullptr || acc->size() == 0) {
      return true;
    }
    auto* flat = acc->childAt(indexOfCountStar_)->asFlatVector<int64_t>();
    VELOX_CHECK_NOT_NULL(
        flat,
        "RetractionRecordCounter expects a flat BIGINT COUNT(*) column at index {}",
        indexOfCountStar_);
    // Flink guarantees the COUNT(*) accumulator is never null, but we tolerate
    // a null value here and treat it as zero to be safe.
    return flat->isNullAt(0) || flat->valueAt(0) == 0;
  }

  int indexOfCountStar() const {
    return indexOfCountStar_;
  }

 private:
  const int indexOfCountStar_;
};

// static
inline std::unique_ptr<RecordCounter> RecordCounter::of(int indexOfCountStar) {
  if (indexOfCountStar >= 0) {
    return std::make_unique<RetractionRecordCounter>(indexOfCountStar);
  }
  return std::make_unique<AccumulationRecordCounter>();
}

} // namespace facebook::velox::stateful
