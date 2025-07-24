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
#include <iostream>

namespace facebook::velox::stateful {

TimeWindow::TimeWindow(
    const TypePtr& inputType,
    const TypePtr& outputType,
    const bool isEventTime,
    const int32_t timeFieldIndex,
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets)
    : StatefulOperator(std::move(op), std::move(targets)),
    inputType_(inputType),
    outputType_(outputType),
    isEventTime_(isEventTime) {
    if (timeFieldIndex < 0) {
        /// TODO: find the time field index.
        const RowTypePtr inputRowType = std::dynamic_pointer_cast<const RowType>(inputType);
        timeFieldIndex_ = inputRowType->children().size() - 1;
    } else {
        timeFieldIndex_ = timeFieldIndex;
    }
}

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

void TimeWindow::close() {
    for (const auto& slice : slices_) {
        slice->close();
    }
    isRunning = false;
}

void TimeWindow::addInput(RowVectorPtr input) {
    const VectorPtr& timeField = input->childAt(timeFieldIndex_);
    VELOX_CHECK(timeField != nullptr);
    std::shared_ptr<SimpleVector<Timestamp>> timestampField = 
        std::dynamic_pointer_cast<SimpleVector<Timestamp>>(timeField);
    VELOX_CHECK(timestampField != nullptr);
    for (size_t i = 0; i < timestampField->size(); ++i) {
        const time_t t = timestampField->valueAt(i).getSeconds();
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

void TimeWindow::getOutput() {
    RowVectorPtr output = std::dynamic_pointer_cast<RowVector>(RowVector::create(outputType_, 0, op()->pool()));
    time_t fireTime = std::time(nullptr);
    if (isEventTime_) {
        VELOX_UNSUPPORTED("Event time window not be supported yet.");
    }
    std::cout << "fireTime:" << fireTime << std::endl;
    for (auto it = slices_.begin(); it != slices_.end(); ) {
        if (fireTime >= (*it)->end_ ) {
            const RowVectorPtr windowOutput = fire((*it));
            output->append(windowOutput.get());
            (*it)->close();
            it = slices_.erase(it);
        } else {
            ++it;
        }
    }
    if (output->size() != 0) {
        pushOutput(output);
    }
}


bool TimeWindow::isFinished() {
    return !isRunning;
}

TumbleTimeWindow::TumbleTimeWindow(
   const TypePtr& inputType,
   const TypePtr& outputType,
   const bool isEventTime,
   const int32_t timeFieldIndex,
   const int64_t windowSize,
   const int64_t offset,
   std::unique_ptr<exec::Operator> op,
   std::vector<std::unique_ptr<StatefulOperator>> targets
) : TimeWindow(inputType, outputType, isEventTime, timeFieldIndex, std::move(op), std::move(targets)), 
size_(windowSize),
offset_(offset) {}

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
        op()->pool(),
        inputRowType,
        outputRowType);
}

const RowVectorPtr TumbleTimeWindow::fire(const std::shared_ptr<TimeSlice>& slice) {
    exec::Window* executor = dynamic_cast<exec::Window *>(op().get());
    const RowVectorPtr windowOutput = slice->getOutput(executor);
    slice->isFired = true;
    return windowOutput;
}

HopTimeWindow::HopTimeWindow(
    const TypePtr& inputType,
    const TypePtr& outputType,
    const bool isEventTime,
    const int32_t timeFieldIndex,
    const int64_t windowSize,
    const int64_t offset,
    const int64_t slidingSize,
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets
) : TimeWindow(inputType, outputType, isEventTime, timeFieldIndex, std::move(op), std::move(targets)),
size_(windowSize),
offset_(offset),
slide_(slidingSize) {}

const std::shared_ptr<TimeSlice> HopTimeWindow::assignTimeSlice(const time_t timestamp) {
    const time_t start = getWindowStartWithOffset(timestamp, offset_, size_);
    const time_t end = start + size_;
    const RowTypePtr inputRowType = std::dynamic_pointer_cast<const RowType>(inputType_);
    const RowTypePtr outputRowType = std::dynamic_pointer_cast<const RowType>(outputType_);
    VELOX_CHECK(inputRowType != nullptr);
    VELOX_CHECK(outputRowType != nullptr);
    return std::make_shared<TimeSlice>(
        start, 
        end,
        op()->pool(),
        inputRowType,
        outputRowType);
}

const RowVectorPtr HopTimeWindow::fire(const std::shared_ptr<TimeSlice>& slice) {
    return nullptr;
}

SessionTimeWindow::SessionTimeWindow(
    const TypePtr& inputType,
    const TypePtr& outputType,
    const bool isEventTime,
    const int32_t timeFieldIndex,
    const int64_t gapSize,
    std::unique_ptr<exec::Operator> op,
    std::vector<std::unique_ptr<StatefulOperator>> targets
) : TimeWindow(inputType, outputType, isEventTime, timeFieldIndex, std::move(op), std::move(targets)),
gap_(gapSize) {}

const std::shared_ptr<TimeSlice> SessionTimeWindow::assignTimeSlice(const time_t timestamp) {
    return nullptr;
}

const RowVectorPtr SessionTimeWindow::fire(const std::shared_ptr<TimeSlice>& slice) {
    return nullptr;
}

}