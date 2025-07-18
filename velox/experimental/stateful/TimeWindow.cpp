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
#include "velox/experimental/stateful/TimeWindow.h"
#include "velox/vector/FlatVector.h"
#include "velox/type/Timestamp.h"

#include <ctime>

namespace facebook::velox::stateful {

TimeWindow::TimeWindow(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const TimeWindowNode>& windowNode)
    : Operator(
          driverCtx,
          windowNode->outputType(),
          operatorId,
          windowNode->id(),
          "TimeWindow",
          std::nullopt),
    inputType_(windowNode->inputType()),
    outputType_(windowNode->outputType()),
    operatorId_(operatorId), 
    driverCtx_(driverCtx),
    windowNode_(windowNode),
    isEventTime_(windowNode_->parameters().isEventTime),
    timeFieldIndex_(windowNode_->parameters().timeFieldIndex),
    delay_(windowNode_->parameters().delay) {}

const time_t TimeWindow::lastSliceEnd() {
    if (slices_.empty()) {
        return std::time(nullptr);
    }
    return slices_.back()->end_;
}

const time_t TimeWindow::getWindowStartWithOffset(const time_t timestamp, const time_t offset, const time_t windowSize) {
    const time_t remainder = (timestamp - offset) % windowSize;
    if (remainder < 0) {
        return timestamp - (remainder + windowSize);
    } else {
        return timestamp - remainder;
    }
}

bool TimeWindow::needsInput() const {
    return isRunning;
}

void TimeWindow::close() {
    for (const auto& slice : slices_) {
        slice->close();
    }
    isRunning = false;
}

void TimeWindow::addInput(RowVectorPtr input) {
    const VectorPtr& timeField = input->childAt(timeFieldIndex_);
    VELOX_CHECK(timeField != nullptr);
    const std::shared_ptr<FlatVector<Timestamp>>& fieldVector = 
        std::dynamic_pointer_cast<FlatVector<Timestamp>>(timeField);
    VELOX_CHECK(fieldVector != nullptr);
    for (size_t i = 0; i < fieldVector->size(); ++i) {
        const time_t t = fieldVector->valueAt(i).getSeconds();
        if (t > lastSliceEnd()) {
            const std::shared_ptr<TimeSlice> timeSlice = assignTimeSlice(t);
            slices_.emplace_back(std::move(timeSlice));
        }
        for (size_t j = 0; j < slices_.size(); ++j) {
            if (t >= slices_[j]->start_ && t <= slices_[j]->end_) {
                slices_[j]->addInput(i);
                if (type() == TimeWindowNode::WindowType::TUMBLE) {
                    break;
                }
            } else {
                continue;
            }
        }
    }
    for (size_t i = 0; i < slices_.size(); ++i) {
        slices_[i]->flush(input);
    }
}

RowVectorPtr TimeWindow::getOutput() {
    RowVectorPtr output;
    time_t fireTime = std::time(nullptr);
    if (isEventTime_) {
        VELOX_UNSUPPORTED("Event time window not be supported yet.");
    }
    for (auto it = slices_.begin(); it != slices_.end(); ) {
        if (fireTime >= (*it)->end_ + delay_) {
            const std::shared_ptr<exec::Window> executor = 
                std::make_shared<exec::Window>(operatorId_, driverCtx_, windowNode_);
            const RowVectorPtr windowOutput = (*it)->getOutput(executor);
            output->append(windowOutput.get());
            executor->close();
            (*it)->close();
            it = slices_.erase(it);
        } else {
            ++it;
        }
    }
    return output;
}

exec::BlockingReason TimeWindow::isBlocked(ContinueFuture* /** future */) {
    return exec::BlockingReason::kNotBlocked;
}

bool TimeWindow::isFinished() {
    return !isRunning;
}

TumbleTimeWindow::TumbleTimeWindow(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const TimeWindowNode>& windowNode
) : TimeWindow(operatorId, driverCtx, windowNode), size_(windowNode->parameters().windowSize), offset_(0) {}

const std::shared_ptr<TimeSlice> TumbleTimeWindow::assignTimeSlice(const time_t timestamp) {
    const time_t start = getWindowStartWithOffset(timestamp, offset_, size_);
    const time_t end = start + size_;
    const RowTypePtr inputRowType = std::dynamic_pointer_cast<const RowType>(inputType_);
    const RowTypePtr outputRowType = std::dynamic_pointer_cast<const RowType>(outputType_);
    VELOX_CHECK(inputRowType != nullptr);
    VELOX_CHECK(outputRowType != nullptr);
    return std::make_shared<TimeSlice>(
        start, 
        end,
        pool(),
        inputRowType,
        outputRowType);
}

}