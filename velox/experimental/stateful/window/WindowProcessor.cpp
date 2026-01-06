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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/experimental/stateful/window/WindowProcessor.h"
#include "velox/experimental/stateful/window/WindowBuffer.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

template<typename K, typename W>
WindowProcessor<K, W>::WindowProcessor(WindowBufferPtr& windowBuffer) : windowBuffer_(windowBuffer) {}

template<typename K, typename W>
SlicingWindowAggProcessor<K, W>::SlicingWindowAggProcessor(
      std::unique_ptr<SliceAssigner>& sliceAssigner,
      std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
      std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
      std::shared_ptr<std::mutex>& mtx,
      WindowBufferPtr& windowBuffer,
      const int32_t shiftTimeZone,
      const int64_t windowInterval,
      const bool useDayLightSaving
) : WindowProcessor<K, W>(windowBuffer),
    sliceAssigner_(std::move(sliceAssigner)),
    windowState_(windowState),
    windowTimerService_(windowTimerService),
    mtx_(mtx),
    shiftTimeZone_(shiftTimeZone),
    windowInterval_(windowInterval),
    useDayLightSaving_(useDayLightSaving) {}

template<typename K, typename W>
bool SlicingWindowAggProcessor<K, W>::processElement(const int64_t progress, const K& key, const RowVectorPtr& data) {
  int64_t currentTime = sliceAssigner_->isEventTime() ? 0 : TimeWindowUtil::getCurrentProcessingTime();
  std::map<int64_t, RowVectorPtr> sliceEndToData = sliceAssigner_->assignSliceEnd(currentTime,data);
  for (const auto& [sliceEnd, data] : sliceEndToData) {
    auto windowData = data;
    if (!sliceAssigner_->isEventTime()) {
      windowTimerService_->registerProcessingTimeTimer(key, sliceEnd, sliceEnd);
    }
    if (sliceAssigner_->isEventTime() && TimeWindowUtil::isWindowFired(sliceEnd, progress, shiftTimeZone_)) {
      // the assigned slice has been triggered, which means current element is late,
      // but maybe not need to drop
      int64_t lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);
      if (TimeWindowUtil::isWindowFired(lastWindowEnd, progress, shiftTimeZone_)) {
        // the last window has been triggered, so the element can be dropped now
        // TODO: record dropped counter.
        continue;
      } else {
        // TODO: addElement may have data output.
        WindowProcessor<K, W>::windowBuffer_->addElement(key, sliceStateMergeTarget(sliceEnd), windowData);
        // we need to register a timer for the next unfired window,
        // because this may the first time we see elements under the key
        int64_t unfiredFirstWindow = sliceEnd;
        while (TimeWindowUtil::isWindowFired(unfiredFirstWindow, progress, shiftTimeZone_)) {
          unfiredFirstWindow += windowInterval_;
        }
        windowTimerService_->registerEventTimeTimer(key, unfiredFirstWindow, unfiredFirstWindow - 1);
      }
    } else {
      std::lock_guard<std::mutex> lock(*mtx_);
      WindowProcessor<K, W>::windowBuffer_->addElement(key, sliceEnd, windowData);
    }
  }
  return true;
}

template<typename K, typename W>
int64_t SlicingWindowAggProcessor<K, W>::sliceStateMergeTarget(int64_t sliceToMerge) {
  // TODO: implement it
  return sliceToMerge;
}

template<typename K, typename W>
void SlicingWindowAggProcessor<K, W>::advanceWatermark(int64_t progress) {
    windowTimerService_->advanceWatermark(progress);
}

/// Add window_start / window_end timestamp to output
RowVectorPtr addWindowTimestampToOutput(
  const RowVectorPtr& output,
  const std::string& fieldName,
  const int64_t fieldValue,
  const int32_t fieldIndex) {
  auto createTimestampVector = [&](
    const Timestamp& val,
    const size_t size,
    velox::memory::MemoryPool* pool) -> VectorPtr {
      const TypePtr windowStartType = std::make_shared<const TimestampType>();
      VectorPtr windowStartVec = BaseVector::create(windowStartType, output->size(), output->pool());
      FlatVector<Timestamp>* timestampVector = windowStartVec->asFlatVector<Timestamp>();
      for (size_t i = 0; i < size; ++i) {
        timestampVector->set(i, val);
      }
      return windowStartVec;
  };
  const TypePtr& outputType = output->type();
  const RowTypePtr& outputRowType = std::dynamic_pointer_cast<const RowType>(outputType);
  const std::vector<std::string>& outputFieldNames = outputRowType->names();
  const std::vector<TypePtr>& outputFieldTypes = outputRowType->children();
  const std::vector<VectorPtr>& outputFields = output->children();
  std::vector<std::string> newOutputFieldNames;
  std::vector<TypePtr> newOutputFieldTypes;
  std::vector<VectorPtr> newOutputFields;
  VectorPtr windowStartVec = createTimestampVector(Timestamp::fromMillis(fieldValue), output->size(), output->pool());
  for (int32_t i = 0; i < fieldIndex; ++i) {
    newOutputFieldTypes.emplace_back(outputFieldTypes[i]);
    newOutputFieldNames.emplace_back(outputFieldNames[i]);
    newOutputFields.emplace_back(outputFields[i]);
  }
  newOutputFieldTypes.emplace_back(std::make_shared<const TimestampType>());
  newOutputFieldNames.emplace_back(fieldName);
  newOutputFields.emplace_back(windowStartVec);
  for (int32_t i = fieldIndex + 1; i < output->childrenSize() + 1; ++i) {
    newOutputFieldTypes.emplace_back(outputFieldTypes[i-1]);
    newOutputFieldNames.emplace_back(outputFieldNames[i-1]);
    newOutputFields.emplace_back(outputFields[i-1]);
  }
  auto newOutputRowType = std::make_shared<const RowType>(std::move(newOutputFieldNames), std::move(newOutputFieldTypes));
  return std::make_shared<RowVector>(output->pool(),
    newOutputRowType,
    output->nulls(),
    output->size(),
    newOutputFields,
    output->getNullCount()
  );
}

template<typename K, typename W>
RowVectorPtr SlicingWindowAggProcessor<K, W>::fireWindow(K key, int64_t timerTimestamp, W windowEnd) {
  RowVectorPtr output = nullptr;
  if (sliceAssigner_->getWindowType() == WindowType::TUMBLE) {
    output = windowState_->value(key, windowEnd);
  } else if (sliceAssigner_->getWindowType() == WindowType::HOP) {
    
  }
  if (!output) {
    LOG(INFO) << "No output found for key: " << key << ", window end: " << windowEnd;
    return nullptr;
  } else {
    if (windowStartIndex_ >= 0) {
      output = addWindowTimestampToOutput(
        output,
        "window_start",
        windowEnd - windowInterval_,
        windowStartIndex_);
    }
    if (windowEndIndex_ >= 0) {
      output = addWindowTimestampToOutput(
        output,
      "window_end",
      windowEnd,
      windowEndIndex_);
    }
    return output;
  }
}

template<typename K, typename W>
void SlicingWindowAggProcessor<K, W>::clearWindow(K key, int64_t timerTimestamp, W windowEnd) {
  std::list<int64_t> expires = sliceAssigner_->expiredSlices(windowEnd);
  for (const int64_t slice : expires) {
    windowState_->remove(key, slice);
  }
}

template<typename K, typename W>
void SlicingWindowAggProcessor<K, W>::clearBuffer() {
    WindowProcessor<K, W>::windowBuffer_->clear();
}

template<typename K, typename W>
void SlicingWindowAggProcessor<K, W>::close() {
  windowTimerService_->close();
  WindowProcessor<K, W>::windowBuffer_->clear();
}

template<typename K, typename W>
SliceUnSharedWindowAggProcessor<K, W>::SliceUnSharedWindowAggProcessor(
  std::unique_ptr<SliceAssigner>& sliceAssigner,
  std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
  std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
  std::shared_ptr<std::mutex>& mtx,
  WindowBufferPtr& windowBuffer,
  const int32_t shiftTimeZone,
  const int64_t windowInterval,
  const bool useDayLightSaving)
 : SlicingWindowAggProcessor<K, W>(
    sliceAssigner, 
    windowState, 
    windowTimerService,
    mtx,
    windowBuffer,
    shiftTimeZone,
    windowInterval,
    useDayLightSaving) {}

template<typename K, typename W>
SliceSharedWindowAggProcessor<K, W>::SliceSharedWindowAggProcessor(
  std::unique_ptr<SliceAssigner>& sliceAssigner,
  std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
  std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
  std::shared_ptr<std::mutex>& mtx,
  WindowBufferPtr& windowBuffer,
  const int32_t shiftTimeZone,
  const int64_t windowInterval,
  const bool useDayLightSaving)
 : SlicingWindowAggProcessor<K, W>(
    sliceAssigner, 
    windowState, 
    windowTimerService,
    mtx,
    windowBuffer,
    shiftTimeZone,
    windowInterval,
    useDayLightSaving) {}

template<typename K, typename W>
inline std::shared_ptr<WindowProcessor<K, W>> buildWindowProgressor(
  std::unique_ptr<SliceAssigner>& sliceAssigner,
  std::shared_ptr<ValueState<K, W, RowVectorPtr>>& windowState,
  std::shared_ptr<InternalTimerService<K, W>>& windowTimerService,
  std::shared_ptr<std::mutex>& mtx,
  WindowBufferPtr& windowBuffer,
  const int32_t shiftTimeZone,
  const int64_t windowInterval,
  const bool useDayLightSaving ) {
  WindowType windowType = sliceAssigner->getWindowType();
  if (windowType == WindowType::TUMBLE) {
    return std::make_shared<SliceUnSharedWindowAggProcessor<K, W>>(
      sliceAssigner, windowState, windowTimerService, mtx, windowBuffer, shiftTimeZone, windowInterval, useDayLightSaving);
  } else if (windowType == WindowType::HOP || windowType == WindowType::CUMULATIVE) {
    return std::make_shared<SliceSharedWindowAggProcessor<K, W>>(
      sliceAssigner, windowState, windowTimerService, mtx, windowBuffer, shiftTimeZone, windowInterval, useDayLightSaving);
  } else {
    return std::make_shared<UnslicingWindowProcessor<K, W>>(windowBuffer);
  }
}

template std::shared_ptr<WindowProcessor<uint32_t, int64_t>> buildWindowProgressor(
  std::unique_ptr<SliceAssigner>& sliceAssigner,
  std::shared_ptr<ValueState<uint32_t, int64_t, RowVectorPtr>>& windowState,
  std::shared_ptr<InternalTimerService<uint32_t, int64_t>>& windowTimerService,
  std::shared_ptr<std::mutex>& mtx,
  WindowBufferPtr& windowBuffer,
  const int32_t shiftTimeZone,
  const int64_t windowInterval,
  const bool useDayLightSaving );
}
