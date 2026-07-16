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
#include <memory>

#include "velox/experimental/stateful/IdleTimerManager.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/WatermarkIdleTracker.h"

namespace facebook::velox::stateful {

/// It is related to
/// org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator
/// in Flink. It extracts timestamp from each row and generates periodic
/// watermark. When an idle timeout is configured, it also detects idle input
/// and emits WatermarkStatus.IDLE / ACTIVE downstream, mirroring Flink's
/// WatermarkAssignerOperator idleness semantics.
class WatermarkAssigner : public StatefulOperator {
 public:
  WatermarkAssigner(
      std::unique_ptr<exec::Operator> op,
      std::vector<std::unique_ptr<StatefulOperator>> targets,
      const int64_t idleTimeout,
      const int rowtimeFieldIndex,
      const int64_t watermarkInterval);

  ~WatermarkAssigner() override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  /// Invoked by the driver thread (via StatefulTask::next) after each drain
  /// attempt. Performs the idle check and emits WatermarkStatus if the input
  /// has just transitioned to idle.
  void checkWatermarkStatus(int64_t now) override;

  std::string name() const override {
    return "WatermarkAssigner";
  }

 private:
  /// Schedules a one-shot idle-detection timer on the background
  /// processing-time scheduler. When the timer fires it wakes the Java mailbox
  /// via the native callback bridge so the driver thread can drain any emitted
  /// WatermarkStatus event.
  void scheduleIdleTimer(int64_t now);

  void shutdownScheduler();

  RowVectorPtr input_;
  const int rowtimeFieldIndex_;
  const int64_t watermarkInterval_;

  int64_t currentWatermark = 0;
  int64_t lastWatermark = 0;

  WatermarkIdleTracker idleTracker_;
  IdleTimerManager idleTimerManager_;
};

} // namespace facebook::velox::stateful
