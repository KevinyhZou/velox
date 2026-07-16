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
#include "velox/experimental/stateful/WatermarkGenerator.h"

namespace facebook::velox::stateful {

/// WatermarkSource wraps a source operator and generates watermarks from the
/// source output using WatermarkGenerator. When an idle timeout is configured
/// on the generator, the source also detects idle input (no source output for
/// the idle timeout duration) and emits WatermarkStatus.IDLE / ACTIVE
/// downstream, mirroring Flink's source-level idleness semantics.
class WatermarkSource : public StatefulOperator {
 public:
  WatermarkSource(
      std::unique_ptr<exec::Operator> op,
      std::vector<StatefulOperatorPtr> targets,
      std::unique_ptr<WatermarkGenerator> watermarkGenerator);

  ~WatermarkSource() override;

  void initialize() override;

  void advance() override;

  void close() override;

  /// Invoked by the driver thread (via StatefulTask::next) after each drain
  /// attempt. Performs the idle check and emits WatermarkStatus if the source
  /// has just transitioned to idle.
  void checkWatermarkStatus(int64_t now) override;

  std::string name() const override {
    return "WatermarkSource";
  }

 private:
  /// Schedules a one-shot idle-detection timer on the background
  /// processing-time scheduler. When the timer fires it wakes the Java mailbox
  /// via the native callback bridge so the driver thread can drain any emitted
  /// WatermarkStatus event.
  void scheduleIdleTimer(int64_t now);

  void shutdownScheduler();

  std::unique_ptr<WatermarkGenerator> watermarkGenerator_;
  IdleTimerManager idleTimerManager_;
};

} // namespace facebook::velox::stateful
