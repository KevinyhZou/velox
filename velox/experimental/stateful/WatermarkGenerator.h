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
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/WatermarkIdleTracker.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::stateful {

/// Shared helpers for watermark extraction (used by WatermarkGenerator and
/// WatermarkAssigner).
namespace watermark {

void validateRowtimeNoNulls(const RowVectorPtr& input, int rowtimeFieldIndex);

RowVectorPtr getTimestampVector(exec::Operator* op, const RowVectorPtr& input);

/// Iterates over timestamps to compute the current watermark.
/// When the watermark crosses the next threshold (lastWatermark +
/// watermarkInterval), the optional emitFn is invoked with
/// (watermark, index) so the caller can handle emission logic
/// (e.g., slicing rows, pushing output, advancing lastWatermark).
/// Returns the updated currentWatermark.
int64_t extractWatermark(
    const int64_t* timestamps,
    vector_size_t timestampSize,
    int64_t currentWatermark,
    int64_t lastWatermark,
    int64_t watermarkInterval,
    const std::function<void(int64_t watermark, vector_size_t index)>& emitFn =
        nullptr);

} // namespace watermark

class WatermarkGenerator {
 public:
  WatermarkGenerator(
      std::unique_ptr<exec::Operator> op,
      const int64_t idleTimeout,
      const int rowtimeFieldIndex,
      const int64_t watermarkInterval);

  /// Feeds one batch through the wrapped operator and updates watermark state.
  /// Do not call addInput on the same operator before this; generate() does it.
  std::optional<int64_t> generate(const RowVectorPtr& input);

  std::string name() const {
    return "WatermarkGenerator";
  }

  void close();

  int64_t currentWatermark() const {
    return currentWatermark_;
  }

  int64_t lastWatermark() const {
    return lastWatermark_;
  }

  /// Notifies the tracker that input data has arrived. Returns true if the
  /// tracker was previously idle (caller should emit WatermarkStatus.ACTIVE).
  bool onRecord(int64_t now) {
    return idleTracker_.onRecord(now);
  }

  /// Checks whether the input has been idle long enough to transition to IDLE.
  /// Returns true if the tracker just became idle (caller should emit
  /// WatermarkStatus.IDLE).
  bool checkIdle(int64_t now) {
    return idleTracker_.checkIdle(now);
  }

  bool isIdle() const {
    return idleTracker_.isIdle();
  }

  int64_t idleDeadline() const {
    return idleTracker_.idleDeadline();
  }

  bool isIdlenessEnabled() const {
    return idleTracker_.isEnabled();
  }

  int64_t idleTimeout() const {
    return idleTimeout_;
  }

  void initialize() {
    op_->initialize();
  }

 private:
  std::unique_ptr<exec::Operator> op_;
  const int64_t idleTimeout_;
  const int rowtimeFieldIndex_;
  const int64_t watermarkInterval_;
  int64_t currentWatermark_ = 0;
  int64_t lastWatermark_ = 0;
  WatermarkIdleTracker idleTracker_;
};
} // namespace facebook::velox::stateful
