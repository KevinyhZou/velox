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

#include <exception>
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/state/SerializedCompositeKeyBuilder.h"
#include "velox/experimental/stateful/TypeSerializer.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace facebook::velox::sateful {

template<typename K, typename N, typename V>
class RocksDBState {
public:
    RocksDBState(rocksdb::DB* db,
        rocksdb::WriteOptions* writeOptions,
        rocksdb::ColumnFamilyHandle* columnFamily,
        std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer,
        std::shared_ptr<stateful::TypeSerializer<V>> valueSerializer) 
        : db_(db), writeOptions_(writeOptions), columnFamily_(columnFamily), namespaceSerializer_(namespaceSerializer), valueSerializer_(valueSerializer)
    {}

    const char* getSerializedValue(
        const char* serializedKeyAndNamespace,
        std::shared_ptr<stateful::TypeSerializer<K>> safeKeySerializer,
        std::shared_ptr<stateful::TypeSerializer<N>> safeNamespaceSerializer,
        std::shared_ptr<stateful::TypeSerializer<V>> safeValueSerializer
    ) {
        K key = safeKeySerializer->deserialize(serializedKeyAndNamespace);
        N ns = safeNamespaceSerializer->deserialize(serializedKeyAndNamespace);
        /// TODO: calculate keyGroup
        int32_t keyGroup = 0;
        std::shared_ptr<stateful::SerializedCompositeKeyBuilder<K>> keyBuilder = 
            std::make_shared<stateful::SerializedCompositeKeyBuilder<K>>(safeKeySerializer, keyGroupPrefixBytes_, 32);
        keyBuilder->setKeyAndKeyGroup(key, keyGroup);
        const char* keyBytes = keyBuilder->buildCompositeKeyNamespace(ns, safeNamespaceSerializer);
        rocksdb::PinnableSlice* value;
        auto status = db_->Get(*readOptions_, columnFamily_, keyBytes, value);
        if (!status.ok()) {
            VELOX_FAIL("Failed to get value by key: {}, namespace: {}", key, ns);
        } else {
            return value->data();
        }
    }

    const char* serializeCurrentKeyWithGroupAndNamespace(K key, N ns) {
        /// TODO: calculate keyGroup
        int32_t keyGroupId = 0;
        sharedKeyAndNamespaceSerializer_->setKeyAndKeyGroup(key, keyGroupId);
        return sharedKeyAndNamespaceSerializer_->buildCompositeKeyNamespace(ns, namespaceSerializer_);
    }

    const char* serializeValue(V value) {
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
        const char* keyBytes = serializeCurrentKeyWithGroupAndNamespace(key, ns);
        try {
            rocksdb::PinnableSlice* value;
            auto status = db_->Get(*readOptions_, columnFamily_, keyBytes, value);
            if (!status.ok()) {
                VELOX_FAIL("Failed to get value by key: {}, namespace:{}", key, ns);
            } else {
                return valueSerializer_->deserialize(value->data());
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to get from rocksdb:{}", e.what());
        }
    }

    const void put(K key, N ns, V value) {
        const char* keyBytes = serializeCurrentKeyWithGroupAndNamespace(key, ns);
        const char* valueBytes = valueSerializer_->serialize(value);
        try {
            auto status = db_->Put(*writeOptions_, columnFamily_, keyBytes, valueBytes);
            if (!status.ok()) {
                VELOX_FAIL("Failed to put value into rocksdb, with key {}, namespace {}, value:{}", key, ns, value);
            }
        } catch (const std::exception& e) {
            VELOX_FAIL("Failed to put into rocksdb:{}", e.what());
        }
    }

    const void remove(K key, N ns) {
        const char* keyBytes = serializeCurrentKeyWithGroupAndNamespace(key, ns);
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
    rocksdb::WriteOptions* writeOptions_;
    rocksdb::ReadOptions* readOptions_;
    N currentNamespace_;
    V defaultValue_;
    int32_t keyGroupPrefixBytes_;
    std::shared_ptr<stateful::TypeSerializer<N>> namespaceSerializer_;
    std::shared_ptr<stateful::TypeSerializer<V>> valueSerializer_;
    std::shared_ptr<stateful::SerializedCompositeKeyBuilder<K>> sharedKeyAndNamespaceSerializer_;
};

template<typename K, typename N, typename V>
class RocksDBValueState : public RocksDBState<K, N, V>, stateful::ValueState<K, N, V> {
public:
    V value(K key, N ns) override {
        return RocksDBState<K, N, V>::get(key, ns);
    }
  
    void update(K key, N ns, V value) override {
        return RocksDBState<K, N, V>::put(key, ns, value);
    }
  
    void remove(K key, N ns) override {
        return RocksDBState<K, N, V>::remove(key, ns);
    }
};

template<typename K, typename N, typename V>
class RocksDBListState : public RocksDBState<K, N, std::list<V>>, stateful::ListState<K, N, V>{
public:
    std::list<V>& get(K key, N ns) override {
        return RocksDBState<K, N, std::list<V>>::get(key, ns);
    }
  
    void add(K key, N ns, V value) override {
        std::list<V>& valueList = RocksDBState<K, N, V>::get(key, ns);
        valueList.emplace_back(value);
        RocksDBState<K, N, V>::put(key, ns, valueList);
    }
  
    void remove(K key, N ns) override {
        return RocksDBState<K, N, V>::remove(key, ns);
    }
};

template<typename K, typename N, typename UK, typename UV>
class RocksDBMapState : public RocksDBState<K, N, std::map<UK, UV>>, stateful::MapState<K, N, UK, UV>{
public:
    UV get(K key, N ns, UK userKey) override {
        std::map<UK, UV> map = RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        return map[userKey];
    }

    void put(K key, N ns, UK userKey, UV value) override {
        std::map<UK, UV> map = RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        map[userKey] = value;
        RocksDBState<K, N, std::map<UK, UV>>::put(key, ns, map);
    }

    std::map<UK, UV> entries(K key, N ns) override {
        return RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
    }

    void remove(K key, N ns, UK userKey) override {
        std::map<UK, UV> map = RocksDBState<K, N, std::map<UK, UV>>::get(key, ns);
        map.erase(userKey);
        RocksDBState<K, N, std::map<UK, UV>>::put(key, ns, map);
    }
};

}
