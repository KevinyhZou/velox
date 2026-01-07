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

#include <exec/Operator.h>
#include <experimental/stateful/StatefulOperator.h>
#include "velox/vector/ComplexVector.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowBuffer.h"
#include "velox/experimental/stateful/InternalTimerService.h"
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace facebook::velox::stateful {

template<typename K, typename W>
class WindowProcessor {
public:
  using WindowStatePtr = std::shared_ptr<ValueState<K, W, RowVectorPtr>>;

  WindowProcessor(
    WindowBufferPtr& windowBuffer,
    WindowStatePtr& windowState);

  // void initializeWatermark(const int64_t watermark);

  virtual bool processElement(const int64_t progress, const K& key, const RowVectorPtr& rowData) {
    return false;
  }

  RowVectorPtr stateValue(K key, W window) {
    return windowState_->value(key, window);
  }

  void stateUpdate(K key, W window, RowVectorPtr value) {
    windowState_->update(key, window, value);
  }

  void stateRemove(K key, W window) {
    windowState_->remove(key, window);
  }

  std::unordered_map<WindowKey, std::list<RowVectorPtr>>& advanceProgress(int64_t progress) {
    return windowBuffer_->advanceProgress(progress);
  }

  virtual void advanceWatermark(int64_t progress) {};

  void setWindowState(WindowStatePtr& windowState) {
    windowState_ = windowState;
  }

  virtual void setWindowTimerService(std::shared_ptr<InternalTimerService<K, W>>& windowTimerService) {}

  // void prepareCheckpoint();

  virtual RowVectorPtr fireWindow(K key, int64_t timerTimestamp, W window, std::unique_ptr<exec::Operator>& op) {
    return nullptr;
  }

  virtual int64_t getNextTriggerWatermark(int64_t progress) {
    return 0;
  }

  virtual void clearWindow(K key, int64_t timerTimestamp, W window) {}

  virtual void clearBuffer() {}

  virtual void close() {}

protected:
  WindowBufferPtr windowBuffer_;
  WindowStatePtr windowState_;
};

template<typename K, typename W>
class SlicingWindowAggProcessor : public stateful::WindowProcessor<K, W> {
public:
    SlicingWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving,
      const int32_t windowStartIndex,
      const int32_t windowEndIndex
    );

    bool processElement(const int64_t progress, const K& key, const RowVectorPtr& data) override;

    void advanceWatermark(int64_t progress) override;

    void clearWindow(K key, int64_t timerTimestamp, W window) override;

    int64_t getNextTriggerWatermark(int64_t progress) override;

    void clearBuffer() override;

    void close() override;

    void setWindowStartAndEnd(RowVectorPtr& data, W windowEnd);

    void setWindowTimerService(std::shared_ptr<InternalTimerService<K, W>>& windowTimerService);

protected:
  std::unique_ptr<SliceAssigner> sliceAssigner_;
  std::shared_ptr<InternalTimerService<K, W>> windowTimerService_;
  const int32_t shiftTimeZone_ ; // TODO: support time zone shift
  const int64_t windowInterval_;
  const bool useDayLightSaving_;
  const int32_t windowStartIndex_;
  const int32_t windowEndIndex_ ;
  
  int64_t sliceStateMergeTarget(int64_t sliceToMerge);
};

template<typename K, typename W>
class SliceUnSharedWindowAggProcessor : public SlicingWindowAggProcessor<K, W>{
public:
    SliceUnSharedWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving,
      const int32_t windowStartIndex,
      const int32_t windowEndIndex);

    RowVectorPtr fireWindow(K key, int64_t timerTimestamp, W window, std::unique_ptr<exec::Operator>& op) override;
};

template<typename K, typename W>
class SliceSharedWindowAggProcessor : public SlicingWindowAggProcessor<K, W>{
public:
    SliceSharedWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving,
      const int32_t windowStartIndex,
      const int32_t windowEndIndex);

    RowVectorPtr fireWindow(K key, int64_t timerTimestamp, W window, std::unique_ptr<exec::Operator>& op) override;

    RowVectorPtr merge(K key, W mergeResult, std::list<W>& toBeMerged, std::unique_ptr<exec::Operator>& op);
};

template<typename K, typename W>
class UnslicingWindowProcessor : public WindowProcessor<K, W> {
public:
  UnslicingWindowProcessor(WindowBufferPtr& windowBuffer, std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState) 
    : WindowProcessor<K, W>(windowBuffer, windowState) {}

};

template<typename K, typename W>
std::shared_ptr<WindowProcessor<K, W>> buildWindowProgressor(
  std::unique_ptr<SliceAssigner> sliceAssigner,
  std::shared_ptr<ValueState<K, W, RowVectorPtr>> windowState,
  std::shared_ptr<InternalTimerService<K, W>> windowTimerService,
  WindowBufferPtr& windowBuffer,
  const int32_t shiftTimeZone,
  const int64_t windowInterval,
  const bool useDayLightSaving,
  const int32_t windowStartIndex,
  const int32_t windowEndIndex);

}