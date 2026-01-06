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

#include "velox/vector/ComplexVector.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowBuffer.h"
#include "velox/experimental/stateful/InternalTimerService.h"
#include <cstdint>
#include <unordered_map>

namespace facebook::velox::stateful {

template<typename K, typename W>
class WindowProcessor {
public:
  WindowProcessor(WindowBufferPtr& windowBuffer);

  // void initializeWatermark(const int64_t watermark);

  virtual bool processElement(const int64_t progress, const K& key, const RowVectorPtr& rowData) {
    return false;
  }

  std::unordered_map<WindowKey, std::list<RowVectorPtr>>& advanceProgress(int64_t progress) {
    return windowBuffer_->advanceProgress(progress);
  }

  virtual void advanceWatermark(int64_t progress) {};

  // void prepareCheckpoint();

  virtual RowVectorPtr fireWindow(K key, int64_t timerTimestamp, W window) {
    return nullptr;
  }

  virtual void clearWindow(K key, int64_t timerTimestamp, W window) {}

  virtual void clearBuffer();

  virtual void close() {}

protected:
  WindowBufferPtr windowBuffer_;
};

template<typename K, typename W>
class SlicingWindowAggProcessor : public stateful::WindowProcessor<K, W> {
public:
    SlicingWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      std::shared_ptr<std::mutex>& mtx,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving
    );

    bool processElement(const int64_t progress, const K& key, const RowVectorPtr& data) override;

    void advanceWatermark(int64_t progress) override;

    RowVectorPtr fireWindow(K key, int64_t timerTimestamp, W window);

    void clearWindow(K key, int64_t timerTimestamp, W window);

    void clearBuffer() override;

    void close() override;

private:
    const int32_t shiftTimeZone_ = 0; // TODO: support time zone shift
    const int64_t windowInterval_;
    const bool useDayLightSaving_;
    const int32_t windowStartIndex_;
    const int32_t windowEndIndex_;
    std::unique_ptr<SliceAssigner> sliceAssigner_;
    std::shared_ptr<ValueState<K, W, RowVectorPtr>> windowState_;
    std::shared_ptr<InternalTimerService<K, W>> windowTimerService_;
    std::shared_ptr<std::mutex> mtx_;

    int64_t sliceStateMergeTarget(int64_t sliceToMerge);
};

template<typename K, typename W>
class SliceUnSharedWindowAggProcessor : public SlicingWindowAggProcessor<K, W>{
public:
    SliceUnSharedWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      std::shared_ptr<std::mutex>& mtx,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving);
};

template<typename K, typename W>
class SliceSharedWindowAggProcessor : public SlicingWindowAggProcessor<K, W>{
public:
    SliceSharedWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      std::shared_ptr<std::mutex>& mtx,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving);
};

template<typename K, typename W>
class UnslicingWindowProcessor : public WindowProcessor<K, W> {
public:
  UnslicingWindowProcessor() {}
};

template<typename K, typename W>
class WindowProcessorBuilder {
public:
  static std::shared_ptr<WindowProcessor<K, W>> buildWindowProgressor(
    std::unique_ptr<SliceAssigner>& sliceAssigner,
    std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
    std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
    std::shared_ptr<std::mutex>& mtx,
    WindowBufferPtr& windowBuffer,
    const int32_t shiftTimeZone,
    const int64_t windowInterval,
    const bool useDayLightSaving);
};

}