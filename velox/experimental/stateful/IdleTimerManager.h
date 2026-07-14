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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "velox/experimental/stateful/ProcessingTimeScheduler.h"

namespace facebook::velox::stateful {

// Shared one-shot idle timer helper for watermark-generating operators. It
// owns the background scheduler, prevents duplicate timer registrations, skips
// scheduling after the owner is already idle, and clamps past idle deadlines to
// the current time.
class IdleTimerManager {
 public:
  using TimerCallback = std::function<void(int64_t)>;

  ~IdleTimerManager() {
    shutdown();
  }

  void schedule(
      int64_t now,
      bool enabled,
      bool idle,
      int64_t idleDeadline,
      TimerCallback callback) {
    if (!enabled || idle || timerPending_.exchange(true)) {
      return;
    }
    if (!scheduler_) {
      scheduler_ = std::make_unique<SystemProcessingTimeScheduler>();
    }
    const int64_t fireAt = idleDeadline < now ? now : idleDeadline;
    scheduler_->registerTimer(
        fireAt,
        ProcessingTimerTask(
            fireAt,
            [this, callback = std::move(callback)](int64_t timestamp) mutable {
              timerPending_ = false;
              callback(timestamp);
            }));
  }

  void shutdown() {
    if (scheduler_) {
      scheduler_->close();
      scheduler_.reset();
    }
    timerPending_ = false;
  }

 private:
  std::unique_ptr<ProcessingTimeScheduler> scheduler_;
  std::atomic<bool> timerPending_{false};
};

} // namespace facebook::velox::stateful
