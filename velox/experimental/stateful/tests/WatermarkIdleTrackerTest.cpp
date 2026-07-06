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
#include "velox/experimental/stateful/WatermarkIdleTracker.h"
#include <gtest/gtest.h>

namespace facebook::velox::stateful::test {
namespace {

TEST(WatermarkIdleTrackerTest, disabledWhenIdleTimeoutNonPositive) {
  WatermarkIdleTracker tracker(0);
  EXPECT_FALSE(tracker.isEnabled());
  EXPECT_FALSE(tracker.onRecord(1000));
  EXPECT_FALSE(tracker.checkIdle(10'000));
  EXPECT_FALSE(tracker.isIdle());

  WatermarkIdleTracker negativeTracker(-1);
  EXPECT_FALSE(negativeTracker.isEnabled());
}

TEST(WatermarkIdleTrackerTest, onRecordReturnsFalseWhenNotIdle) {
  WatermarkIdleTracker tracker(5'000);
  EXPECT_FALSE(tracker.onRecord(1'000));
  EXPECT_FALSE(tracker.isIdle());
}

TEST(WatermarkIdleTrackerTest, emitsIdleWhenTimeoutElapses) {
  WatermarkIdleTracker tracker(5'000);
  tracker.onRecord(1'000);
  EXPECT_FALSE(tracker.checkIdle(4'000));
  EXPECT_FALSE(tracker.isIdle());
  EXPECT_TRUE(tracker.checkIdle(6'001));
  EXPECT_TRUE(tracker.isIdle());
}

TEST(WatermarkIdleTrackerTest, doesNotEmitIdleTwice) {
  WatermarkIdleTracker tracker(5'000);
  tracker.onRecord(1'000);
  EXPECT_TRUE(tracker.checkIdle(6'001));
  EXPECT_FALSE(tracker.checkIdle(20'000));
  EXPECT_TRUE(tracker.isIdle());
}

TEST(WatermarkIdleTrackerTest, onRecordReactivatesFromIdle) {
  WatermarkIdleTracker tracker(5'000);
  tracker.onRecord(1'000);
  tracker.checkIdle(6'001);
  EXPECT_TRUE(tracker.isIdle());

  EXPECT_TRUE(tracker.onRecord(7'000));
  EXPECT_FALSE(tracker.isIdle());
  EXPECT_FALSE(tracker.onRecord(8'000));
}

TEST(WatermarkIdleTrackerTest, checkIdleAfterReactivationDoesNotFireEarly) {
  WatermarkIdleTracker tracker(5'000);
  tracker.onRecord(1'000);
  tracker.checkIdle(6'001);
  tracker.onRecord(7'000);
  EXPECT_FALSE(tracker.checkIdle(9'000));
  EXPECT_FALSE(tracker.isIdle());
  EXPECT_TRUE(tracker.checkIdle(12'001));
}

TEST(WatermarkIdleTrackerTest, firstCheckSetsBaselineWithoutFiring) {
  WatermarkIdleTracker tracker(5'000);
  EXPECT_FALSE(tracker.checkIdle(100'000));
  EXPECT_FALSE(tracker.isIdle());
  tracker.onRecord(101'000);
  EXPECT_FALSE(tracker.checkIdle(103'000));
  EXPECT_TRUE(tracker.checkIdle(106'001));
}

TEST(WatermarkIdleTrackerTest, exactTimeoutDoesNotFire) {
  WatermarkIdleTracker tracker(5'000);
  tracker.onRecord(1'000);
  EXPECT_FALSE(tracker.checkIdle(6'000));
  EXPECT_FALSE(tracker.isIdle());
}

TEST(WatermarkIdleTrackerTest, setIdleTransitionsState) {
  WatermarkIdleTracker tracker(5'000);
  tracker.setIdle(true);
  EXPECT_TRUE(tracker.isIdle());
  tracker.setIdle(false);
  EXPECT_FALSE(tracker.isIdle());
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
