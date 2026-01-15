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

#include "velox/experimental/stateful/state/RocksDBKeyedStateBackend.h"
#include "velox/vector/ComplexVector.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/RocksDBState.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include <memory>

namespace facebook::velox::stateful {

RocksDBKeyedStateBackend::RocksDBKeyedStateBackend(
    rocksdb::DB* db,
    const rocksdb::ReadOptions* readOptions,
    const rocksdb::WriteOptions* writeOptions,
    const std::list<std::string>& states,
    const std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*>& stateColumnFamilies,
    const std::unordered_map<std::string, std::string>& stateOperators
    // const std::unordered_map<std::string, TypePtr>& stateKeys,
    // const std::unordered_map<std::string, TypePtr>& stateNamespaces,
    /// const std::unordered_map<std::string, TypePtr>& stateValues,
    // memory::MemoryPool* pool
    ) 
    : KeyedStateBackend(), db_(db),
    readOptions_(readOptions),
    writeOptions_(writeOptions),
    states_(states),
    // stateKeys_(stateKeys),
    // stateNamespaces_(stateNamespaces),
    // stateValues_(stateValues),
    stateOperators_(stateOperators),
    stateColumnFamilies_(stateColumnFamilies) {}

void RocksDBKeyedStateBackend::checkValidState(const std::string& stateName) {
    auto stateIt = std::find(states_.begin(), states_.end(), stateName);
    if (stateIt == states_.end()) {
        VELOX_FAIL("The rocksdb state {} is not registered", stateName);
    }
    auto stateCFIt = std::find(stateColumnFamilies_.begin(), stateColumnFamilies_.end(), stateName);
    if (stateCFIt == stateColumnFamilies_.end()) {
        VELOX_FAIL("No column family related to rocksdb state {}", stateName);
    }
    auto stateKeyIt = std::find(stateKeys_.begin(), stateKeys_.end(), stateName);
    if (stateKeyIt == stateKeys_.end()) {
        VELOX_FAIL("No state key related to rocksdb state {}", stateName);
    }
    auto stateNamespaceIt = std::find(stateNamespaces_.begin(), stateNamespaces_.end(), stateName);
    if (stateNamespaceIt == stateNamespaces_.end()) {
        VELOX_FAIL("No state namespace related to rocksdb state {}", stateName);
    }
    auto stateValueIt = std::find(stateValues_.begin(), stateValues_.end(), stateName);
    if (stateValueIt == stateValues_.end()) {
        VELOX_FAIL("No state value related to rocksdb state {}", stateName);
    }
}

std::shared_ptr<ValueState<uint32_t, int64_t, RowVectorPtr>> RocksDBKeyedStateBackend::getOrCreateValueState(StateDescriptor& stateDescriptor) {
    const std::string stateName = stateDescriptor.name();
    
    checkValidState(stateName);

    const std::string operatorId = stateDescriptor.operatorId();
    if (stateOperators_[stateName] != operatorId) {
        VELOX_FAIL("The rocksdb state {} is not matched with the operatorId: {}", stateName, operatorId);
    }
    using B = rocksdb::Slice;
    std::shared_ptr<ValueSerializer<uint32_t, B>> keySerializer = 
        std::dynamic_pointer_cast<ValueSerializer<uint32_t, B>>(createSerializer<B>(stateKeys_[stateName], pool_));
    std::shared_ptr<ValueSerializer<int64_t, B>> namespaceSerializer =
        std::dynamic_pointer_cast<ValueSerializer<int64_t, B>>(createSerializer<B>(stateNamespaces_[stateName], pool_));
    std::shared_ptr<ComplexVectorSerializer<RowVectorPtr, B>> valueSerializer =
        std::dynamic_pointer_cast<ComplexVectorSerializer<RowVectorPtr, B>>(createSerializer<B>(stateValues_[stateName], pool_));
    return std::make_shared<RocksDBValueState<uint32_t, int64_t, RowVectorPtr>>(
        db_, readOptions_, writeOptions_, stateColumnFamilies_[stateName],keySerializer, namespaceSerializer, valueSerializer);
}

std::shared_ptr<ListState<uint32_t, long, RowVectorPtr>> RocksDBKeyedStateBackend::getOrCreateListState(StateDescriptor& stateDescriptor) {
    return nullptr;
}

std::shared_ptr<MapState<uint32_t, int, RowVectorPtr, int>> RocksDBKeyedStateBackend::getOrCreateMapState(StateDescriptor& stateDescriptor) {
    return nullptr;
}

std::shared_ptr<ValueState<uint32_t, TimeWindow, RowVectorPtr>> RocksDBKeyedStateBackend::getOrCreateGroupValueState(StateDescriptor& stateDescriptor) {
    return nullptr;
}

std::shared_ptr<MapState<uint32_t, int, TimeWindow, TimeWindow>> RocksDBKeyedStateBackend::getOrCreateGroupMapState(StateDescriptor& stateDescriptor) {
    return nullptr;
}

std::shared_ptr<MapState<uint32_t, int, uint32_t, RowVectorPtr>> RocksDBKeyedStateBackend::getOrCreateRankMapState(StateDescriptor& stateDescriptor) {
    return nullptr;
}

std::shared_ptr<InternalTimerService<uint32_t, long>> RocksDBKeyedStateBackend::createTimerService(Triggerable<uint32_t, long>* triggerable) {
    return std::make_shared<InternalTimerService<uint32_t, long>>(triggerable);
}

std::shared_ptr<InternalTimerService<uint32_t, TimeWindow>> RocksDBKeyedStateBackend::createGroupWindowAggTimerService(Triggerable<uint32_t, TimeWindow>* triggerable) {
    return nullptr;
}

void RocksDBKeyedStateBackend::snapshot(long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {}

void RocksDBKeyedStateBackend::notifyCheckpointComplete(long checkpointId) {}

void RocksDBKeyedStateBackend::notifyCheckpointAborted(long checkpointId) {}

}
