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

#include "velox/common/memory/MemoryPool.h"
#include "velox/core/PlanNode.h"
#include "velox/core/Expressions.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Window.h"
#include "velox/exec/Driver.h"
#include "velox/type/Type.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"

namespace facebook::velox::stateful {

class TimeSlice {
public:
    TimeSlice(
        const time_t start,
        const time_t end,
        memory::MemoryPool* memoryPool,
        const RowTypePtr& inputType,
        const RowTypePtr& outputType) :
        start_(start),
        end_(end),
        inputType_(inputType),
        outputType_(outputType),
        input_(RowVector::createEmpty(inputType_, memoryPool)) {}
    
    const time_t start_;
    const time_t end_;
    const RowTypePtr inputType_;
    const RowTypePtr outputType_;
    mutable RowVectorPtr input_;
    mutable std::vector<vector_size_t> inputSelector_;

    void addInput(const size_t index) {
        inputSelector_.emplace_back(index);
    }

    void flush(const RowVectorPtr& source) {
        VELOX_CHECK(source != nullptr);
        const size_t currentInputSize = input_->size();
        input_->resize(currentInputSize + inputSelector_.size());
        SelectivityVector selectVector(inputSelector_.size());
        for (auto i = 0; i < inputSelector_.size(); ++i) {
            selectVector.setValid(currentInputSize + i, true);
        }
        input_->copy(source.get(), selectVector, &inputSelector_[0]);
        inputSelector_.clear();
    }

    const RowVectorPtr getOutput(const std::shared_ptr<exec::Window>& executor) {
        executor->addInput(input_);
        const auto result = executor->getOutput();
        input_->prepareForReuse();
        return result;
    }

    void close() {
    }
};


class TimeWindow : public exec::Operator {
public:
  TimeWindow(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const TimeWindowNode>& windowNode);

  bool needsInput() const override;
  /// Add input for operator, assign the input values to different windows.
  void addInput(RowVectorPtr input) override;
  /// Get output for operator, judge and fire the window to get output values.
  RowVectorPtr getOutput() override;
  exec::BlockingReason isBlocked(ContinueFuture* future) override;
  bool isFinished() override;
  void close() override;
  
  virtual const TimeWindowNode::WindowType type() = 0;
protected:
    const TypePtr inputType_;
    const TypePtr outputType_;
    const time_t lastSliceEnd();
    const time_t getWindowStartWithOffset(const time_t timestamp, const time_t offset, const time_t windowSize);
    virtual const std::shared_ptr<TimeSlice> assignTimeSlice(const time_t timestamp) = 0;
private:
    mutable bool isRunning = true;
    const int32_t operatorId_;
    exec::DriverCtx* driverCtx_;
    const std::shared_ptr<const TimeWindowNode> windowNode_;
    const bool isEventTime_;
    const int32_t timeFieldIndex_;
    const int32_t delay_;
    mutable std::vector<std::shared_ptr<TimeSlice>> slices_;
};

class TumbleTimeWindow : public TimeWindow {
public:
    TumbleTimeWindow(
        int32_t operatorId,
        exec::DriverCtx* driverCtx,
        const std::shared_ptr<const TimeWindowNode>& windowNode);
    
    const TimeWindowNode::WindowType type() override {
        return TimeWindowNode::WindowType::TUMBLE;
    }

protected:
    const std::shared_ptr<TimeSlice> assignTimeSlice(const time_t timestamp) override;

private:
    time_t size_;
    time_t offset_ = 0;
};
}