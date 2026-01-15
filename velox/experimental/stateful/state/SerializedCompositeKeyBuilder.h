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

#include "velox/experimental/stateful/TypeSerializer.h"
#include "rocksdb/slice.h"
#include <cstdint>

namespace facebook::velox::stateful {

template<typename K>
class SerializedCompositeKeyBuilder {
public:
    SerializedCompositeKeyBuilder(
        const std::shared_ptr<TypeSerializer<K, rocksdb::Slice>> keySerializer,
        int32_t keyGroupPrefixBytes,
        int32_t initialSize) {

    }
    void setKeyAndKeyGroup(K key, int32_t keyGroupId) {
        
    }

    template<typename N>
    void setNamespace(N ns, const std::shared_ptr<TypeSerializer<N, rocksdb::Slice>> namespaceSerializer) {

    }

    template<typename N>
    const rocksdb::Slice buildCompositeKeyNamespace(N ns, const std::shared_ptr<TypeSerializer<N, rocksdb::Slice>> namespaceSerializer) {
        return nullptr;
    }
};

}
