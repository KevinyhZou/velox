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
#include "velox/experimental/stateful/WatermarkIdleTracker.h"

namespace facebook::velox::stateful {

WatermarkIdleTracker::WatermarkIdleTracker(int64_t idleTimeoutMs)
    : idleTimeout_(idleTimeoutMs) {}

bool WatermarkIdleTracker::onRecord(int64_t currentWallMs) {
  if (!isEnabled()) {
    return false;
  }
  lastRecordWallTime_ = currentWallMs;
  if (idle_) {
    idle_ = false;
    return true;
  }
  return false;
}

bool WatermarkIdleTracker::checkIdle(int64_t currentWallMs) {
  if (!isEnabled() || idle_) {
    return false;
  }
  if (lastRecordWallTime_ == 0) {
    // No record has ever arrived. Treat the first check as the baseline so we
    // do not immediately declare idleness before any data has been seen.
    lastRecordWallTime_ = currentWallMs;
    return false;
  }
  if (currentWallMs - lastRecordWallTime_ > idleTimeout_) {
    idle_ = true;
    return true;
  }
  return false;
}

bool WatermarkIdleTracker::isIdle() const {
  return idle_;
}

bool WatermarkIdleTracker::isEnabled() const {
  return idleTimeout_ > 0;
}

void WatermarkIdleTracker::setIdle(bool idle) {
  idle_ = idle;
}

} // namespace facebook::velox::stateful
