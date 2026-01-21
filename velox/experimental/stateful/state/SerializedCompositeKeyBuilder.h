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

#include <sys/socket.h>
#include "velox/experimental/stateful/TypeSerializer.h"
#include "rocksdb/slice.h"
#include <cstdint>
#include <memory>

namespace facebook::velox::stateful {

template<typename K, typename N>
class SerializedCompositeKeyBuilder {
public:
    SerializedCompositeKeyBuilder(
        const std::shared_ptr<TypeSerializer<K, rocksdb::Slice>> keySerializer,
        const std::shared_ptr<TypeSerializer<N, rocksdb::Slice>> namespaceSerializer,
        int32_t keyGroupPrefixBytes,
        int32_t initialSize) :
        keySerializer_(keySerializer),
        namespaceSerializer_(namespaceSerializer),
        keyGroupPrefixBytes_(keyGroupPrefixBytes),
        initialSize_(initialSize) { }

    const std::string buildCompositeKeyNamespace(K key, N ns) {
        rocksdb::Slice keySlice = keySerializer_->serialize(key);
        rocksdb::Slice namespaceSlice = namespaceSerializer_->serialize(ns);
        std::string compositeString;
        compositeString.reserve(keySlice.size() + namespaceSlice.size());
        compositeString.append(keySlice.data(), keySlice.size());
        compositeString.append(namespaceSlice.data(), namespaceSlice.size());
        return compositeString;
    }

private:
    const std::shared_ptr<TypeSerializer<K, rocksdb::Slice>> keySerializer_;
    const std::shared_ptr<TypeSerializer<N, rocksdb::Slice>> namespaceSerializer_;
    const int32_t keyGroupPrefixBytes_;
    const int32_t initialSize_;
};

}
