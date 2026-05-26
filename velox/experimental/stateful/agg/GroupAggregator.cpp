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
#include "velox/experimental/stateful/agg/GroupAggregator.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/JoinedRowVector.h"
#include <cstdint>

namespace facebook::velox::stateful {

GroupAggregator::GroupAggregator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& aggNode,
    std::unique_ptr<AggsHandleFunction> aggsFunction,
    int64_t stateRetentionTime,
    bool generateUpdateBefore,
    int32_t indexOfCountStar)
    : exec::Operator(
          driverCtx,
          aggNode->outputType(),
          operatorId,
          aggNode->id(),
          "GroupAggregator"),
      aggsFunction_(std::move(aggsFunction)),
      stateRetentionTime_(stateRetentionTime),
      generateUpdateBefore_(generateUpdateBefore),
      recordCounter_(RecordCounter::of(indexOfCountStar)) {}

void GroupAggregator::open(StreamOperatorStateHandler* stateHandler) {
  StateDescriptor stateDesc("agg-deduplicate-state");
  // TODO: support ttl
  // StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
  // if (ttlConfig.isEnabled()) {
  //    stateDesc.enableTimeToLive(ttlConfig);
  // }
  accState_ = stateHandler->getValueState(stateDesc);
  aggsFunction_->open(stateHandler);
}

RowVectorPtr GroupAggregator::processElements(
    int64_t key,
    RowVectorPtr input) {
  bool firstRow;
  RowVectorPtr accumulators = accState_->value(key, State::VOID_NAMESPACE);
  if (!accumulators) {
    if (stateful::isRetractMsg(input)) {
      // TODO: Implement retract logic.
      return nullptr;
    }
    firstRow = true;
    accumulators = aggsFunction_->createAccumulators();
  } else {
    firstRow = false;
  }

  aggsFunction_->setAccumulators(accumulators);
  RowVectorPtr prevAggValue = aggsFunction_->getValue();

  if (stateful::isAccumulateMsg(input)) {
    aggsFunction_->accumulate(input);
  } else {
    aggsFunction_->retract(input);
  }

  RowVectorPtr newAggValue = aggsFunction_->getValue();
  accumulators = aggsFunction_->getAccumulators();

  if (!recordCounter_->recordCountIsZero(accumulators)) {
    accState_->update(key, State::VOID_NAMESPACE, accumulators);
    if (!firstRow) {
      if (stateRetentionTime_ <= 0 && stateful::equalRowVectors(prevAggValue, newAggValue)) { 
        return nullptr;
      } else {
        if (generateUpdateBefore_) {
          /// TODO: retract the previous row
          return nullptr;
        }
        return newAggValue;
      }
    } else {
      return newAggValue;
    }
  } else {
    if (!firstRow) {
      return nullptr;
    }
    accState_->clear();
    aggsFunction_->cleanup();
  }
  return nullptr;
}

void GroupAggregator::close() {
  exec::Operator::close();
  accState_->clear();
  accState_.reset();
}
} // namespace facebook::velox::stateful
