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

#include "velox/experimental/stateful/StreamRecordTimestampInserter.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::stateful::test {
namespace {

class TimestampPassthroughOperator : public exec::Operator {
 public:
  explicit TimestampPassthroughOperator(exec::DriverCtx* driverCtx)
      : Operator(
            driverCtx,
            ROW({"rowtime"}, {TIMESTAMP()}),
            0,
            "timestamp_passthrough",
            "TimestampPassthrough") {}

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    VELOX_CHECK_NOT_NULL(input_);
    auto output = std::make_shared<RowVector>(
        input_->pool(),
        outputType_,
        nullptr,
        input_->size(),
        std::vector<VectorPtr>{input_->childAt(0)});
    input_.reset();
    return output;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

 private:
  RowVectorPtr input_;
};

class NoOutputOperator : public exec::Operator {
 public:
  explicit NoOutputOperator(exec::DriverCtx* driverCtx)
      : Operator(
            driverCtx,
            ROW({"rowtime"}, {TIMESTAMP()}),
            0,
            "spy_target",
            "SpyTarget") {}

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }
};

class SpyStatefulOperator : public StatefulOperator {
 public:
  explicit SpyStatefulOperator(exec::DriverCtx* driverCtx)
      : StatefulOperator(std::make_unique<NoOutputOperator>(driverCtx), {}) {}

  void addInput(StreamElementPtr input) override {
    if (input->isRecord()) {
      auto record = std::static_pointer_cast<StreamRecord>(input);
      recordSizes_.push_back(record->record()->size());
      timestamps_.push_back(record->timestamp());
      hasTimestamps_.push_back(record->hasTimestamp());
    }
  }

  void advance() override {}

  const std::vector<vector_size_t>& recordSizes() const {
    return recordSizes_;
  }

  const std::vector<int64_t>& timestamps() const {
    return timestamps_;
  }

  const std::vector<bool>& hasTimestamps() const {
    return hasTimestamps_;
  }

 private:
  std::vector<vector_size_t> recordSizes_;
  std::vector<int64_t> timestamps_;
  std::vector<bool> hasTimestamps_;
};

class StreamRecordTimestampInserterTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{tsBatch({0})});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "StreamRecordTimestampInserterTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
    driver_ = exec::Driver::testingCreate();
    driverCtx_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
    OperatorTestBase::TearDown();
  }

  RowVectorPtr tsBatch(const std::vector<int64_t>& millis) {
    std::vector<Timestamp> ts;
    ts.reserve(millis.size());
    for (auto m : millis) {
      ts.push_back(Timestamp::fromMillis(m));
    }
    return makeRowVector({makeFlatVector<Timestamp>(ts)});
  }

  RowVectorPtr nullableTsBatch(
      const std::vector<std::optional<int64_t>>& millis) {
    std::vector<std::optional<Timestamp>> ts;
    ts.reserve(millis.size());
    for (auto m : millis) {
      ts.push_back(
          m.has_value() ? std::make_optional(Timestamp::fromMillis(*m))
                        : std::nullopt);
    }
    return makeRowVector({makeNullableFlatVector<Timestamp>(ts, TIMESTAMP())});
  }

  std::unique_ptr<StreamRecordTimestampInserter> makeInserter(
      std::unique_ptr<SpyStatefulOperator> spy,
      int rowtimeFieldIndex) {
    std::vector<StatefulOperatorPtr> targets;
    targets.push_back(std::move(spy));
    return std::make_unique<StreamRecordTimestampInserter>(
        std::make_unique<TimestampPassthroughOperator>(driverCtx_.get()),
        std::move(targets),
        rowtimeFieldIndex);
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

TEST_F(StreamRecordTimestampInserterTest, emitsBatchMaxTimestamp) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  auto inserter = makeInserter(std::move(spy), /*rowtimeFieldIndex=*/0);

  inserter->addInput(
      std::make_shared<StreamRecord>("input", tsBatch({100, 200, 50, 150})));
  inserter->advance();

  ASSERT_EQ(1, spyPtr->recordSizes().size());
  EXPECT_EQ(4, spyPtr->recordSizes()[0]);
  ASSERT_EQ(1, spyPtr->timestamps().size());
  EXPECT_EQ(200, spyPtr->timestamps()[0]);
  ASSERT_EQ(1, spyPtr->hasTimestamps().size());
  EXPECT_TRUE(spyPtr->hasTimestamps()[0]);
}

TEST_F(StreamRecordTimestampInserterTest, emitsNoOutputWithoutInput) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  auto inserter = makeInserter(std::move(spy), /*rowtimeFieldIndex=*/0);

  inserter->advance();

  EXPECT_TRUE(spyPtr->recordSizes().empty());
  EXPECT_TRUE(spyPtr->timestamps().empty());
}

TEST_F(StreamRecordTimestampInserterTest, rejectsNullRowtime) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto inserter = makeInserter(std::move(spy), /*rowtimeFieldIndex=*/0);

  inserter->addInput(std::make_shared<StreamRecord>(
      "input", nullableTsBatch({100, std::nullopt, 2000})));
  VELOX_ASSERT_THROW(
      inserter->advance(), "RowTime field should not have nulls");
}

TEST_F(StreamRecordTimestampInserterTest, handlesSingleRowBatch) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  auto inserter = makeInserter(std::move(spy), /*rowtimeFieldIndex=*/0);

  inserter->addInput(std::make_shared<StreamRecord>("input", tsBatch({1234})));
  inserter->advance();

  ASSERT_EQ(1, spyPtr->recordSizes().size());
  EXPECT_EQ(1, spyPtr->recordSizes()[0]);
  EXPECT_EQ(1234, spyPtr->timestamps()[0]);
}

TEST_F(StreamRecordTimestampInserterTest, handlesMultipleBatchesIndependently) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  auto inserter = makeInserter(std::move(spy), /*rowtimeFieldIndex=*/0);

  inserter->addInput(
      std::make_shared<StreamRecord>("input", tsBatch({100, 300})));
  inserter->advance();
  inserter->addInput(
      std::make_shared<StreamRecord>("input", tsBatch({50, 80})));
  inserter->advance();

  ASSERT_EQ(2, spyPtr->recordSizes().size());
  EXPECT_EQ(300, spyPtr->timestamps()[0]);
  EXPECT_EQ(80, spyPtr->timestamps()[1]);
}

TEST_F(StreamRecordTimestampInserterTest, emitsEmptyBatchWithoutTimestamp) {
  auto spy = std::make_unique<SpyStatefulOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  auto inserter = makeInserter(std::move(spy), /*rowtimeFieldIndex=*/0);

  inserter->addInput(std::make_shared<StreamRecord>("input", tsBatch({})));
  inserter->advance();

  ASSERT_EQ(1, spyPtr->recordSizes().size());
  EXPECT_EQ(0, spyPtr->recordSizes()[0]);
  ASSERT_EQ(1, spyPtr->hasTimestamps().size());
  EXPECT_FALSE(spyPtr->hasTimestamps()[0]);
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
