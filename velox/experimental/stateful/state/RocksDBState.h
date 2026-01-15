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

#include "velox/vector/ComplexVector.h"
#include <exception>
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/SerializedCompositeKeyBuilder.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace facebook::velox::stateful {

template<typename K, typename N, typename V>
class RocksDBState {
public:
    RocksDBState(
        rocksdb::DB* db,
        const rocksdb::ReadOptions* readOptions,
        const rocksdb::WriteOptions* writeOptions,
        rocksdb::ColumnFamilyHandle* columnFamily,
        const std::shared_ptr<stateful::TypeSerializer<K, rocksdb::Slice>> keySerializer,
        const std::shared_ptr<stateful::TypeSerializer<N, rocksdb::Slice>> namespaceSerializer,
        const std::shared_ptr<stateful::TypeSerializer<V, rocksdb::Slice>> valueSerializer) 
        : db_(db),
        columnFamily_(columnFamily),
        readOptions_(readOptions),
        writeOptions_(writeOptions),
        keySerializer_(keySerializer),
        namespaceSerializer_(namespaceSerializer),
        valueSerializer_(valueSerializer)
    {}

    // const rocksdb::Slice getSerializedValue(
    //     const char* serializedKeyAndNamespace,
    //     std::shared_ptr<stateful::TypeSerializer<K, rocksdb::Slice>> safeKeySerializer,
    //     std::shared_ptr<stateful::TypeSerializer<N, rocksdb::Slice>> safeNamespaceSerializer,
    //     std::shared_ptr<stateful::TypeSerializer<V, rocksdb::Slice>> safeValueSerializer
    // ) {
    //     K key = safeKeySerializer->deserialize(serializedKeyAndNamespace);
    //     N ns = safeNamespaceSerializer->deserialize(serializedKeyAndNamespace);
    //     /// TODO: calculate keyGroup
    //     int32_t keyGroup = 0;
    //     std::shared_ptr<stateful::SerializedCompositeKeyBuilder<K>> keyBuilder = 
    //         std::make_shared<stateful::SerializedCompositeKeyBuilder<K>>(safeKeySerializer, keyGroupPrefixBytes_, 32);
    //     keyBuilder->setKeyAndKeyGroup(key, keyGroup);
    //     const char* keyBytes = keyBuilder->buildCompositeKeyNamespace(ns, safeNamespaceSerializer);
    //     rocksdb::PinnableSlice* value;
    //     auto status = db_->Get(*readOptions_, columnFamily_, keyBytes, value);
    //     if (!status.ok()) {
    //         VELOX_FAIL("Failed to get value by key: {}, namespace: {}", key, ns);
    //     } else {
    //         return value->data();
    //     }
    // }

    const rocksdb::Slice serializeCurrentKeyWithGroupAndNamespace(K key, N ns) {
        /// TODO: calculate keyGroup
        int32_t keyGroupId = 0;
        sharedKeyAndNamespaceSerializer_->setKeyAndKeyGroup(key, keyGroupId);
        return sharedKeyAndNamespaceSerializer_->buildCompositeKeyNamespace(ns, namespaceSerializer_);
    }

    const rocksdb::Slice serializeValue(V value) {
        return valueSerializer_->serialize(value);
    }

    void clear(K key) {
        try {
            db_->Delete(writeOptions_, columnFamily_, serializeCurrentKeyWithGroupAndNamespace(key, currentNamespace_));
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to clear rocksdb, {}", e.what());
        }
    }

    V get(K key, N ns) {
        const rocksdb::Slice keyBytes = serializeCurrentKeyWithGroupAndNamespace(key, ns);
        try {
            rocksdb::PinnableSlice* value = nullptr;
            auto status = db_->Get(*readOptions_, columnFamily_, keyBytes, value);
            if (!status.ok() || !value) {
                VELOX_FAIL("Failed to get value by key: {}, namespace:{}", key, ns);
            } else {
                return valueSerializer_->deserialize(value->data());
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to get from rocksdb:{}", e.what());
        }
    }

    const void put(K key, N ns, V value) {
        const rocksdb::Slice keyBytes = serializeCurrentKeyWithGroupAndNamespace(key, ns);
        const rocksdb::Slice valueBytes = valueSerializer_->serialize(value);
        try {
            auto status = db_->Put(*writeOptions_, columnFamily_, keyBytes, valueBytes);
            if (!status.ok()) {
                VELOX_FAIL("Failed to put value into rocksdb, with key {}, namespace {}", key, ns);
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to put into rocksdb:{}", e.what());
        }
    }

    const void remove(K key, N ns) {
        const rocksdb::Slice keyBytes = serializeCurrentKeyWithGroupAndNamespace(key, ns);
        try {
            auto status = db_->Delete(*writeOptions_, columnFamily_, keyBytes);
            if (!status.ok()) {
                VELOX_FAIL("Failed to remove from rocksdb:{}, with key {}, namespace:{}", key, ns);
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to remove from rocksdb:{}", e.what());
        }
    }

protected:
    rocksdb::DB* db_;
    rocksdb::ColumnFamilyHandle* columnFamily_;
    const rocksdb::ReadOptions* readOptions_;
    const rocksdb::WriteOptions* writeOptions_;
    N currentNamespace_;
    V defaultValue_;
    int32_t keyGroupPrefixBytes_;
    const std::shared_ptr<stateful::TypeSerializer<K, rocksdb::Slice>> keySerializer_;
    const std::shared_ptr<stateful::TypeSerializer<N, rocksdb::Slice>> namespaceSerializer_;
    const std::shared_ptr<stateful::TypeSerializer<V, rocksdb::Slice>> valueSerializer_;
    const std::shared_ptr<stateful::SerializedCompositeKeyBuilder<K>> sharedKeyAndNamespaceSerializer_;
};

template<typename K, typename N, typename V>
class RocksDBValueState : public RocksDBState<K, N, V>, public ValueState<K, N, V> {
public:
    RocksDBValueState(
        rocksdb::DB* db,
        const rocksdb::ReadOptions* readOptions,
        const rocksdb::WriteOptions* writeOptions,
        rocksdb::ColumnFamilyHandle* columnFamily,
        const std::shared_ptr<stateful::TypeSerializer<K, rocksdb::Slice>> keySerializer,
        const std::shared_ptr<stateful::TypeSerializer<N, rocksdb::Slice>> namespaceSerializer,
        const std::shared_ptr<stateful::TypeSerializer<V, rocksdb::Slice>> valueSerializer)
    : RocksDBState<K, N, V>(db, readOptions, writeOptions, columnFamily, keySerializer, namespaceSerializer, valueSerializer) {}

    V value(K key, N ns) override {
        return RocksDBState<K, N, V>::get(key, ns);
    }
  
    void update(K key, N ns, V value) override {
        return RocksDBState<K, N, V>::put(key, ns, value);
    }
  
    void remove(K key, N ns) override {
        return RocksDBState<K, N, V>::remove(key, ns);
    }

    void clear() override {}
};

template<typename K, typename N, typename V>
class RocksDBListState : public RocksDBState<K, N, ArrayVectorPtr>, public stateful::ListState<K, N, V>{
public:
    RocksDBListState(
        rocksdb::DB* db,
        rocksdb::ReadOptions* readOptions,
        rocksdb::WriteOptions* writeOptions,
        rocksdb::ColumnFamilyHandle* columnFamily,
        std::shared_ptr<stateful::TypeSerializer<K, rocksdb::Slice>> keySerializer,
        std::shared_ptr<stateful::TypeSerializer<N, rocksdb::Slice>> namespaceSerializer,
        std::shared_ptr<stateful::TypeSerializer<ArrayVectorPtr, rocksdb::Slice>> valueSerializer)
    : RocksDBState<K, N, ArrayVectorPtr>(db, readOptions, writeOptions, columnFamily, keySerializer, namespaceSerializer, valueSerializer) {}
    
    std::list<V>& get(K key, N ns) override {
        // std::list<V> result;
        // ArrayVectorPtr arrayVector = RocksDBState<K, N, ArrayVectorPtr>::get(key, ns);
        // const VectorPtr& elements = arrayVector->elements();
        // for (size_t i = 0; i < arrayVector->size(); ++i) {
        //     arrayVector->asFlatVector<typename T>()
        // }
        // return result;
        return {};
    }
  
    void add(K key, N ns, V value) override {
        // std::list<V>& valueList = RocksDBState<K, N, V>::get(key, ns);
        // valueList.emplace_back(value);
        // RocksDBState<K, N, V>::put(key, ns, valueList);
    }
  
    void remove(K key, N ns) override {
        // return RocksDBState<K, N, V>::remove(key, ns);
    }

    void clear() override {}
};

template<typename K, typename N, typename UK, typename UV>
class RocksDBMapState : public RocksDBState<K, N, MapVectorPtr>, public stateful::MapState<K, N, UK, UV>{
public:
    RocksDBMapState(
        rocksdb::DB* db,
        rocksdb::ReadOptions* readOptions,
        rocksdb::WriteOptions* writeOptions,
        rocksdb::ColumnFamilyHandle* columnFamily,
        std::shared_ptr<stateful::TypeSerializer<K, rocksdb::Slice>> keySerializer,
        std::shared_ptr<stateful::TypeSerializer<N, rocksdb::Slice>> namespaceSerializer,
        std::shared_ptr<stateful::TypeSerializer<MapVectorPtr, rocksdb::Slice>> valueSerializer)
    : RocksDBState<K, N, MapVectorPtr>(db, readOptions, writeOptions, columnFamily, keySerializer, namespaceSerializer, valueSerializer) {}

    UV get(K key, N ns, UK userKey) override {
        // std::map<UK, UV> map = RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        // return map[userKey];
        std::map<UK, UV> map;
        return map[userKey];
    }

    void put(K key, N ns, UK userKey, UV value) override {
        // std::map<UK, UV> map = RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        // map[userKey] = value;
        // RocksDBState<K, N, std::map<UK, UV>>::put(key, ns, map);
    }

    std::map<UK, UV> entries(K key, N ns) override {
        // return RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        return {{}};
    }

    void remove(K key, N ns, UK userKey) override {
        // std::map<UK, UV> map = RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        // map.erase(userKey);
        // RocksDBState<K, N, std::map<UK, UV>>::put(key, ns, map);
    }

    void clear() override {}
};

}
