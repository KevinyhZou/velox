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
#include <list>
#include <memory>
#include "velox/vector/ComplexVector.h"
#include "velox/experimental/stateful/WindowAggregator.h"
#include <experimental/stateful/window/Window.h>
#include <experimental/stateful/window/WindowBuffer.h>
#include <experimental/stateful/window/WindowProcessor.h>
#include "velox/experimental/stateful/TimerHeapInternalTimer.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

WindowAggregator::WindowAggregator(
    std::unique_ptr<exec::Operator> localAggregator,
    std::unique_ptr<exec::Operator> globalAggregator,
    std::vector<std::unique_ptr<StatefulOperator>> targets,
    std::unique_ptr<KeySelector> keySelector,
    std::unique_ptr<SliceAssigner> sliceAssigner,
    const int64_t windowInterval,
    const bool useDayLightSaving,
    const bool isEventTime,
    const int32_t windowStartIndex,
    const int32_t windowEndIndex)
    : StatefulOperator(std::move(globalAggregator), std::move(targets)), Triggerable<uint32_t, int64_t>(),
      localAggregator_(std::move(localAggregator)),
      keySelector_(std::move(keySelector)),
      isEventTime_(isEventTime) {
  
  WindowBufferPtr windowBuffer = std::make_shared<RecordsWindowBuffer>();
  windowProcessor_ = stateful::buildWindowProgressor<uint32_t, int64_t>(
    std::move(sliceAssigner),
    nullptr,
    nullptr,
    windowBuffer,
    0, 
    windowInterval, 
    useDayLightSaving,
    windowStartIndex,
    windowEndIndex);
}

void WindowAggregator::initialize() {
  StatefulOperator::initialize();
  if (localAggregator_) {
    localAggregator_->initialize();
  }
  StateDescriptor stateDesc("window-aggs");
  auto windowState = stateHandler()->getValueState(stateDesc);
  auto windowTimerService = stateHandler()->createTimerService(this);
  windowProcessor_->setWindowState(windowState);
  windowProcessor_->setWindowTimerService(windowTimerService);
}

void WindowAggregator::addInput(RowVectorPtr input) {
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = input;
}

void WindowAggregator::getOutput() {
  if (!input_) {
    return;
  }
  std::map<int64_t, RowVectorPtr> keyToData = keySelector_->partition(input_);
  for (const auto& [key, data] : keyToData) {
    std::lock_guard<std::mutex> lock(*mtx_);
    windowProcessor_->processElement(currentProgress_,  key, data);
  }
  input_.reset();
}

void WindowAggregator::processWatermarkInternal(int64_t timestamp) {
  if (isEventTime_ && timestamp > currentProgress_) {
    currentProgress_ = timestamp;
    if (currentProgress_ >= nextTriggerWatermark_) {
      // we only need to call advanceProgress() when current watermark may trigger window
      auto windowKeyToData = windowProcessor_->advanceProgress(currentProgress_);
      for (const auto&[windowKey, datas] : windowKeyToData) {
        if (datas.empty()) {
          continue;
        }
        // TODO: agg should output no matter how many rows in datas.
        localAggregator_->addInput(TimeWindowUtil::mergeVectors(datas, op()->pool()));
        RowVectorPtr localAcc = localAggregator_->getOutput(); 
        auto stateAcc = windowProcessor_->stateValue(windowKey.key(), windowKey.window());
        std::list<RowVectorPtr> allDatas;
        if (!localAcc && !stateAcc) {
          continue;
        } else {
          if (localAcc) {
            allDatas.push_back(localAcc);
          }
          if (stateAcc) {
            allDatas.push_back(stateAcc);
          }
          op()->addInput(TimeWindowUtil::mergeVectors(allDatas, op()->pool()));
          auto newAcc = op()->getOutput();
          if (newAcc) {
            windowProcessor_->stateUpdate(windowKey.key(), windowKey.window(), newAcc);
          }
        }
      }
      windowProcessor_->advanceWatermark(currentProgress_);
      nextTriggerWatermark_ = windowProcessor_->getNextTriggerWatermark(currentProgress_);
    }
  }
}

void WindowAggregator::onTimer(std::shared_ptr<TimerHeapInternalTimer<uint32_t, int64_t>> timer) {
  const RowVectorPtr output = windowProcessor_->fireWindow(timer->key(), timer->timestamp(), timer->ns(), op());
  if (output) {
    pushOutput(output);
  }
  windowProcessor_->clearWindow(timer->key(), timer->timestamp(), timer->ns());
}

void WindowAggregator::onEventTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, int64_t>> timer) {
  onTimer(timer);
}

void WindowAggregator::onProcessingTime(std::shared_ptr<TimerHeapInternalTimer<uint32_t, int64_t>> timer) {
  if (timer->timestamp() > lastTriggeredProcessingTime_) {
    lastTriggeredProcessingTime_ = timer->timestamp();
    auto windowKeyToData = windowProcessor_->advanceProgress(timer->timestamp());
    for (const auto& [windowKey, datas] : windowKeyToData) {
      if (datas.empty()) {
        continue;
      }
      std::list<RowVectorPtr> allDatas(datas.begin(), datas.end());
      auto stateAcc = windowProcessor_->stateValue(windowKey.key(), windowKey.window());
      if (stateAcc) {
        allDatas.push_back(stateAcc);
      }
      RowVectorPtr opInput = TimeWindowUtil::mergeVectors(allDatas, op()->pool());
      op()->addInput(opInput);
      auto newAcc = op()->getOutput();
      if (newAcc) {
        windowProcessor_->stateUpdate(windowKey.key(), windowKey.window(), newAcc);
      }
    }
    if (!windowKeyToData.empty()) {
      windowProcessor_->clearBuffer();
    }
  }
  onTimer(timer);
}

void WindowAggregator::close() {
  processWatermarkInternal(std::numeric_limits<int64_t>::max());
  StatefulOperator::close();
  if (localAggregator_) {
    localAggregator_->close();
  }
  input_.reset();
  windowProcessor_->close();
  currentProgress_ = 0;
  nextTriggerWatermark_ = 0;
}

} // namespace facebook::velox::stateful
