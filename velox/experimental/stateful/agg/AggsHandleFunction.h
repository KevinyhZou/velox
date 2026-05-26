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

#include "velox/experimental/stateful/state/StreamOperatorStateHandler.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

/// This class corresponds to Flink AggsHandleFunctionBase + AggsHandleFunction
/// (see org.apache.flink.table.runtime.generated.AggsHandleFunctionBase /
/// AggsHandleFunction). It is the entry point for aggregate operators to
/// operate on aggregation state.
///
/// Typical per-key lifecycle driven by a streaming aggregator:
///   1. createAccumulators() if no state exists yet, otherwise read previous
///      accumulators from state.
///   2. setAccumulators(prev) to load the state into this object.
///   3. accumulate(input) and/or retract(input) and/or merge(otherAcc).
///   4. getValue() to extract the user-visible result.
///   5. getAccumulators() to persist updated state back to the state backend.
///   6. cleanup() / close() on tear-down.
///
/// In Flink the concrete subclass is code-generated per query. In this C++
/// port subclasses can be hand-written or generated from the planner. A
/// minimal reference implementation (CountAggsHandleFunction) is provided
/// below.
class AggsHandleFunction {
 public:
  virtual ~AggsHandleFunction() = default;

  /// Initializes the function. Called once before any other working method.
  /// 'store' provides access to keyed state. The concrete implementation may
  /// register additional StateDescriptors here.
  virtual void open(StreamOperatorStateHandler* store) = 0;

  /// Accumulates the input rows into the current accumulators.
  virtual void accumulate(RowVectorPtr input) = 0;

  /// Retracts the input rows from the current accumulators. Used by streaming
  /// queries that support retract messages.
  virtual void retract(RowVectorPtr input) = 0;

  /// Merges another accumulator row into the current accumulators.
  virtual void merge(RowVectorPtr accumulators) = 0;

  /// Restores the current accumulators. Streaming aggregators call this with
  /// the value loaded from state before processing inputs for a given key.
  virtual void setAccumulators(RowVectorPtr accumulators) = 0;

  /// Resets all accumulators to their initial values.
  virtual void resetAccumulators() = 0;

  /// Returns a snapshot of the current accumulators that can be persisted to
  /// state. Must return a non-null RowVectorPtr.
  virtual RowVectorPtr getAccumulators() = 0;

  /// Creates a fresh accumulator row initialized to default values. This is
  /// used when no previous state exists for a key.
  virtual RowVectorPtr createAccumulators() = 0;

  /// Releases any state that backs the retired accumulators.
  virtual void cleanup() = 0;

  /// Tear-down hook.
  virtual void close() = 0;

  /// Returns the user-visible result computed from the current accumulators.
  virtual RowVectorPtr getValue() = 0;

  /// Sets the window size. Only used by aggregations (such as PERCENT_RANK)
  /// that need to know the total rows in the current window frame.
  virtual void setWindowSize(int windowSize) = 0;
};

/// Reference implementation of AggsHandleFunction performing COUNT(*) on
/// the input rows. The accumulator and output schema is a single BIGINT
/// column named "count". This is mainly intended as a working starting
/// point and for unit tests; code-generated subclasses will replace it
/// for real queries.
class CountAggsHandleFunction : public AggsHandleFunction {
 public:
  explicit CountAggsHandleFunction(memory::MemoryPool* pool)
      : pool_(pool), accumulatorType_(ROW({"count"}, {BIGINT()})) {
    VELOX_CHECK_NOT_NULL(pool_, "CountAggsHandleFunction requires a pool");
  }

  void open(StreamOperatorStateHandler* /*store*/) override {}

  void accumulate(RowVectorPtr input) override {
    if (input == nullptr) {
      return;
    }
    count_ += input->size();
    initialized_ = true;
  }

  void retract(RowVectorPtr input) override {
    if (input == nullptr) {
      return;
    }
    count_ -= input->size();
  }

  void merge(RowVectorPtr accumulators) override {
    if (accumulators == nullptr || accumulators->size() == 0) {
      return;
    }
    auto* flat = accumulators->childAt(0)->asFlatVector<int64_t>();
    VELOX_CHECK_NOT_NULL(
        flat, "CountAggsHandleFunction expects flat BIGINT accumulator");
    if (!flat->isNullAt(0)) {
      count_ += flat->valueAt(0);
      initialized_ = true;
    }
  }

  void setAccumulators(RowVectorPtr accumulators) override {
    if (accumulators == nullptr || accumulators->size() == 0) {
      resetAccumulators();
      return;
    }
    auto* flat = accumulators->childAt(0)->asFlatVector<int64_t>();
    VELOX_CHECK_NOT_NULL(
        flat, "CountAggsHandleFunction expects flat BIGINT accumulator");
    if (flat->isNullAt(0)) {
      count_ = 0;
      initialized_ = false;
    } else {
      count_ = flat->valueAt(0);
      initialized_ = true;
    }
  }

  void resetAccumulators() override {
    count_ = 0;
    initialized_ = false;
  }

  RowVectorPtr getAccumulators() override {
    return buildSingleRow(/*forceNull=*/false);
  }

  RowVectorPtr createAccumulators() override {
    // Initial accumulator: count = 0, not null. Matches Flink's behavior
    // where COUNT(*) starts at 0 rather than NULL.
    auto acc = buildSingleRow(/*forceNull=*/false);
    auto* flat = acc->childAt(0)->asFlatVector<int64_t>();
    flat->set(0, 0);
    flat->setNull(0, false);
    return acc;
  }

  void cleanup() override {
    resetAccumulators();
  }

  void close() override {}

  RowVectorPtr getValue() override {
    return buildSingleRow(/*forceNull=*/!initialized_);
  }

  void setWindowSize(int /*windowSize*/) override {}

 private:
  RowVectorPtr buildSingleRow(bool forceNull) const {
    auto child = BaseVector::create(BIGINT(), 1, pool_);
    auto* flat = child->asFlatVector<int64_t>();
    if (forceNull) {
      flat->set(0, 0);
      flat->setNull(0, true);
    } else {
      flat->set(0, count_);
      flat->setNull(0, false);
    }
    return std::make_shared<RowVector>(
        pool_,
        accumulatorType_,
        nullptr,
        1,
        std::vector<VectorPtr>{std::move(child)});
  }

  memory::MemoryPool* const pool_;
  const RowTypePtr accumulatorType_;
  int64_t count_{0};
  bool initialized_{false};
};

} // namespace facebook::velox::stateful
