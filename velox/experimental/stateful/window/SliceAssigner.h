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

#include <memory>
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/window/Window.h"

namespace facebook::velox::stateful {

class WindowAssigner {
public:
  virtual bool isEventTime() = 0;
};

/// This class is relevent to flink SliceAssginer.
class SliceAssigner : public WindowAssigner {
public:
 SliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t offset,
    WindowType windowType,
    int rowtimeIndex)
    : WindowAssigner(), keySelector_(std::move(keySelector)),
      size_(size),
      offset_(offset),
      windowType_(windowType),
      rowtimeIndex_(rowtimeIndex) {}

  virtual std::map<int64_t, RowVectorPtr> assignSliceEnd(const int64_t timestampMs, const RowVectorPtr& input) {
    return {{}};
  }

  virtual int64_t getLastWindowEnd(int64_t sliceEnd) {
    return 0;
  }

  virtual int64_t getWindowStart(int64_t windowEnd) {
    return 0;
  }

  virtual std::list<int64_t> expiredSlices(int64_t windowEnd) {
    return {};
  }

  virtual int64_t getSliceEndInterval() {
    return 0;
  }

  bool isEventTime() override {
    return rowtimeIndex_ >= 0;
  }

  WindowType getWindowType() {
    return windowType_;
  }

 protected:
  const std::unique_ptr<KeySelector> keySelector_;
  const int64_t size_;
  const int64_t offset_;
  const WindowType windowType_;
  int32_t rowtimeIndex_;
};

class TumblingSliceAssigner : public SliceAssigner {
public:
  TumblingSliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t offset,
    int32_t rowtimeIndex);
  
  std::list<int64_t> expiredSlices(int64_t windowEnd) override;

  std::map<int64_t, RowVectorPtr> assignSliceEnd(const int64_t timestampMs, const RowVectorPtr& input) override;

  int64_t getLastWindowEnd(int64_t sliceEnd) override;

  int64_t getSliceEndInterval() override;

  int64_t getWindowStart(int64_t windowEnd) override;
};

class SharedSliceAssigner : public SliceAssigner {
public:
  SharedSliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t step,
    int64_t offset,
    WindowType windowType,
    int32_t rowtimeIndex);

  std::map<int64_t, RowVectorPtr> assignSliceEnd(const int64_t timestampMs, const RowVectorPtr& input) override;

  int64_t getSliceEndInterval() override;

protected:
    int64_t step_;
    int64_t sliceSize_;
};

class HoppingSliceAssigner : public SharedSliceAssigner {
public:
  HoppingSliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t step,
    int64_t offset,
    int32_t rowtimeIndex);
  
  std::list<int64_t> expiredSlices(int64_t windowEnd) override;

  int64_t getLastWindowEnd(int64_t sliceEnd) override;

  int64_t getWindowStart(int64_t windowEnd) override;
};

class CumulativeSliceAssigner : public SharedSliceAssigner {
public:
  CumulativeSliceAssigner(
    std::unique_ptr<KeySelector> keySelector,
    int64_t size,
    int64_t maxSize,
    int64_t step,
    int64_t offset,
    int rowtimeIndex);

  std::list<int64_t> expiredSlices(int64_t windowEnd) override;

  int64_t getLastWindowEnd(int64_t sliceEnd) override;

  int64_t getWindowStart(int64_t windowEnd) override;

private:
    int64_t maxSize_;
};

std::unique_ptr<SliceAssigner> buildSliceAssigner(
  std::unique_ptr<KeySelector> keySelector,
  int64_t size,
  int64_t step,
  int64_t offset,
  WindowType windowType,
  int64_t rowtimeIndex);

} // namespace facebook::velox::stateful
