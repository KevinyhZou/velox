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
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/Window.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"

namespace facebook::velox::stateful {

TumblingSliceAssigner::TumblingSliceAssigner(
  std::unique_ptr<KeySelector> keySelector,
  int64_t size,
  int64_t offset,
  int rowtimeIndex) 
  : SliceAssigner(std::move(keySelector), size, offset, WindowType::TUMBLE, rowtimeIndex) {}

std::list<int64_t> TumblingSliceAssigner::expiredSlices(int64_t windowEnd) {
  std::list<int64_t> expired;
  expired.emplace_back(windowEnd);
  return expired;
}

int64_t TumblingSliceAssigner::getLastWindowEnd(int64_t sliceEnd) {
  return sliceEnd;
}

std::map<int64_t, RowVectorPtr> TumblingSliceAssigner::assignSliceEnd(const int64_t timestampMs, const RowVectorPtr& input) {
  if (!isEventTime()) {
    int64_t utcTimestamp = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
    int64_t windowStart = stateful::TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset_, size_);
    return {{windowStart + size_, input}};
  } else {
    return keySelector_->partition(input);
  }
}

int64_t TumblingSliceAssigner::getSliceEndInterval() {
  return size_;
}

int64_t TumblingSliceAssigner::getWindowStart(int64_t windowEnd) {
  return windowEnd - size_;
}

SharedSliceAssigner::SharedSliceAssigner(
  std::unique_ptr<KeySelector> keySelector,
  int64_t size,
  int64_t step,
  int64_t offset,
  WindowType windowType,
  int32_t rowtimeIndex
) : SliceAssigner(std::move(keySelector), size, offset, windowType, rowtimeIndex), step_(step) {}

std::map<int64_t, RowVectorPtr> SharedSliceAssigner::assignSliceEnd(const int64_t timestampMs, const RowVectorPtr& input) {
  if (!isEventTime()) {
    int64_t utcTimestamp = TimeWindowUtil::toEpochMillsForTimer(timestampMs, 0);
    int64_t windowStart = stateful::TimeWindowUtil::getWindowStartWithOffset(utcTimestamp, offset_, sliceSize_);
    return {{windowStart + sliceSize_, input}};
  } else {
    return keySelector_->partition(input);
  }
}

int64_t SharedSliceAssigner::getSliceEndInterval() {
  return sliceSize_;
}

HoppingSliceAssigner::HoppingSliceAssigner(
  std::unique_ptr<KeySelector> keySelector,
  int64_t size,
  int64_t step,
  int64_t offset,
  int32_t rowtimeIndex
) : SharedSliceAssigner(std::move(keySelector), size, step, offset, WindowType::HOP, rowtimeIndex) {
  sliceSize_ = std::gcd(size, step);
}

std::list<int64_t> HoppingSliceAssigner::expiredSlices(int64_t windowEnd) {
  std::list<int64_t> expired;
  int64_t windowStart = getWindowStart(windowEnd);
  int64_t firstSliceEnd = windowStart + sliceSize_;
  expired.emplace_back(firstSliceEnd);
  return expired;
}

int64_t HoppingSliceAssigner::getLastWindowEnd(int64_t sliceEnd) {
  return sliceEnd - sliceSize_ + size_;
}

int64_t HoppingSliceAssigner::getWindowStart(int64_t windowEnd) {
  return windowEnd - size_;
}

CumulativeSliceAssigner::CumulativeSliceAssigner(
  std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t maxSize,
    int64_t step,
    int64_t offset,
    int rowtimeIndex
) : SharedSliceAssigner(std::move(keySelector), size, step, offset, WindowType::CUMULATIVE, rowtimeIndex), maxSize_(maxSize) {
  sliceSize_ = step;
}

std::list<int64_t> CumulativeSliceAssigner::expiredSlices(int64_t windowEnd) {
  std::list<int64_t> expired;
  int64_t windowStart = getWindowStart(windowEnd);
  int64_t firstSliceEnd = windowStart + step_;
  int64_t lastSliceEnd = windowStart + maxSize_;
  if (windowEnd == firstSliceEnd) {
    return expired;
  } else if (windowEnd == lastSliceEnd) {
    expired.emplace_back(windowEnd);
    expired.emplace_back(firstSliceEnd);
  } else {
    expired.emplace_back(windowEnd);
  }
  return expired;
}

int64_t CumulativeSliceAssigner::getLastWindowEnd(int64_t sliceEnd) {
  int64_t windowStart = getWindowStart(sliceEnd);
  return windowStart + maxSize_;
}

int64_t CumulativeSliceAssigner::getWindowStart(int64_t windowEnd) {
  return TimeWindowUtil::getWindowStartWithOffset(windowEnd - 1, offset_, maxSize_);
}

std::unique_ptr<SliceAssigner> buildSliceAssigner(
  std::unique_ptr<KeySelector> keySelector,
  int64_t size,
  int64_t step,
  int64_t offset,
  WindowType windowType,
  int64_t rowtimeIndex) {
  if (windowType == WindowType::TUMBLE) {
    return std::make_unique<TumblingSliceAssigner>(
      std::move(keySelector),
      size,
      offset,
      rowtimeIndex);
  } else if (windowType == WindowType::HOP) {
    return std::make_unique<HoppingSliceAssigner>(
      std::move(keySelector),
      size,
      step,
      offset,
      rowtimeIndex);
  } else if (windowType == WindowType::CUMULATIVE) {
    return std::make_unique<CumulativeSliceAssigner>(
      std::move(keySelector),
      size,
      0, /// TODO: set maxsize
      step,
      offset,
      rowtimeIndex);
  } else {
    VELOX_FAIL("Window type {} not supported.", static_cast<int32_t>(windowType));
  }
}

} // namespace facebook::velox::stateful
