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

#include "velox/experimental/stateful/IdleTimerManager.h"

#include <gtest/gtest.h>

namespace facebook::velox::stateful::test {
namespace {

class FakeProcessingTimeScheduler : public ProcessingTimeScheduler {
 public:
  std::optional<std::string> registerTimer(
      int64_t timestamp,
      ProcessingTimerTask task) override {
    timestamps_.push_back(timestamp);
    tasks_.push_back(std::move(task));
    return std::to_string(timestamps_.size());
  }

  void fireLast() {
    tasks_.back()();
  }

  const std::vector<int64_t>& timestamps() const {
    return timestamps_;
  }

 private:
  std::vector<int64_t> timestamps_;
  std::vector<ProcessingTimerTask> tasks_;
};

TEST(IdleTimerManagerTest, schedulesAtIdleDeadline) {
  auto scheduler = std::make_unique<FakeProcessingTimeScheduler>();
  auto* schedulerPtr = scheduler.get();
  IdleTimerManager manager(std::move(scheduler));

  std::vector<int64_t> fired;
  manager.schedule(
      1'000,
      true,
      false,
      1'100,
      [&](int64_t timestamp) { fired.push_back(timestamp); });

  EXPECT_EQ((std::vector<int64_t>{1'100}), schedulerPtr->timestamps());
  schedulerPtr->fireLast();
  EXPECT_EQ((std::vector<int64_t>{1'100}), fired);
}

TEST(IdleTimerManagerTest, doesNotRearmWhileTimerIsPending) {
  auto scheduler = std::make_unique<FakeProcessingTimeScheduler>();
  auto* schedulerPtr = scheduler.get();
  IdleTimerManager manager(std::move(scheduler));

  manager.schedule(1'000, true, false, 1'100, [](int64_t) {});
  manager.schedule(1'050, true, false, 1'150, [](int64_t) {});

  EXPECT_EQ((std::vector<int64_t>{1'100}), schedulerPtr->timestamps());
}

TEST(IdleTimerManagerTest, canRearmAfterTimerFires) {
  auto scheduler = std::make_unique<FakeProcessingTimeScheduler>();
  auto* schedulerPtr = scheduler.get();
  IdleTimerManager manager(std::move(scheduler));

  manager.schedule(1'000, true, false, 1'100, [](int64_t) {});
  schedulerPtr->fireLast();
  manager.schedule(1'200, true, false, 1'300, [](int64_t) {});

  EXPECT_EQ((std::vector<int64_t>{1'100, 1'300}), schedulerPtr->timestamps());
}

TEST(IdleTimerManagerTest, alreadyIdleDoesNotArmTimer) {
  auto scheduler = std::make_unique<FakeProcessingTimeScheduler>();
  auto* schedulerPtr = scheduler.get();
  IdleTimerManager manager(std::move(scheduler));

  manager.schedule(1'000, true, true, 1'100, [](int64_t) {});

  EXPECT_TRUE(schedulerPtr->timestamps().empty());
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
