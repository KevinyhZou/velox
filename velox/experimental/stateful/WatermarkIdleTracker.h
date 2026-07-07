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

namespace facebook::velox::stateful {

/// Tracks input idleness for a watermark-generating operator
/// (WatermarkAssigner / WatermarkGenerator), mirroring Flink's
/// WatermarkAssignerOperator idleness semantics.
///
/// A non-positive idleTimeout disables idleness detection.
///
/// All methods are non-thread-safe and must be invoked from the same thread
/// that drives the owning operator. The background timer is responsible only
/// for waking up the Java mailbox via NativeCallbackBridge; the actual idle
/// state transition is performed here, on the driver thread, during the drain
/// triggered by that wake-up.
class WatermarkIdleTracker {
 public:
  explicit WatermarkIdleTracker(int64_t idleTimeoutMs);

  /// Notifies that input data has arrived at the given wall-clock time.
  /// Returns true if the tracker was previously idle, signalling the caller
  /// to emit WatermarkStatus.ACTIVE.
  bool onRecord(int64_t currentWallMs);

  /// Periodically checks whether the input has been idle long enough to
  /// transition to the IDLE status. Returns true if the tracker just became
  /// idle, signalling the caller to emit WatermarkStatus.IDLE. Returns false
  /// when idleness detection is disabled, when the tracker is already idle, or
  /// when the idle timeout has not yet elapsed.
  bool checkIdle(int64_t currentWallMs);

  bool isIdle() const;

  bool isEnabled() const;

  void setIdle(bool idle);

 private:
  const int64_t idleTimeout_;
  int64_t lastRecordWallTime_{0};
  bool idle_{false};
  bool baselineInitialized_{false};
};

} // namespace facebook::velox::stateful
