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
#include "velox/type/Timestamp.h"
#include "velox/type/StringView.h"
#include "velox/type/HugeInt.h"
#include <cstdint>
#include <type_traits>

namespace facebook::velox::stateful {

template<typename T>
class TypeSerializer {
public:
    /// Serialize the given value to char array.
    virtual const char* serialize(const T& t)  = 0;

    /// Deserialize the give char array to value.
    virtual const T deserialize(const char* chs) = 0;
};

template<typename T>
class ValueSerializer : public TypeSerializer<T> {
public:
    const char* serialize(const T& t) override {
        if constexpr (
            std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
            std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
            std::is_same_v<T, int128_t> || std::is_same_v<T, uint8_t> ||
            std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t> ||
            std::is_same_v<T, uint64_t> || std::is_same_v<T, uint128_t> ||
            std::is_same_v<T, float> || std::is_same_v<T, double>) {
            char chs[sizeof(T)];
            std::memcpy(chs, &t, sizeof(T));
            return chs;
        } else if constexpr (std::is_same_v<T, bool>) {
            uint8_t byteValue = t ? 1 : 0;
            char chs[sizeof(uint8_t)];
            std::memcpy(chs, &byteValue, sizeof(uint8_t));
            return chs;
        } else if constexpr (std::is_same_v<T, StringView>) {
            const uint64_t len = t.size();
            char chs[sizeof(uint64_t) + len];
            std::memcpy(chs, &len, sizeof(uint64_t));
            std::memcpy(chs + sizeof(uint64_t), t.data(), len);
            return chs;
        } else if constexpr (std::is_same_v<T, Timestamp>) {
            int64_t mills = t.toMills();
            char chs[sizeof(int64_t)];
            std::memcpy(chs, &t, sizeof(int64_t));
            return chs;
        } else {
            VELOX_FAIL("Type {} is not supported", typeid(T).name());
        }
    }

    const T deserialize(const char* chs) override {
        if constexpr (
            std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
            std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
            std::is_same_v<T, int128_t> || std::is_same_v<T, uint8_t> ||
            std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t> ||
            std::is_same_v<T, uint64_t> || std::is_same_v<T, uint128_t> ||
            std::is_same_v<T, float> || std::is_same_v<T, double>) {
            T t;
            std::memcpy(&t, chs, sizeof(T));
            return t;
        } else if constexpr (std::is_same_v<T, bool>) {
            uint8_t byteValue;
            std::memcpy(&byteValue, chs, sizeof(uint8_t));
            return byteValue > 0 ? true : false;
        } else if constexpr (std::is_same_v<T, StringView>) {
            uint64_t len;
            std::memcpy(&len, chs, sizeof(uint64_t));
            return StringView(chs + sizeof(uint64_t), len);
        } else if constexpr (std::is_same_v<T, Timestamp>) {
            int64_t mills;
            std::memcpy(&mills, chs, sizeof(int64_t));
            return Timestamp::fromMillis(mills);
        } else {
            VELOX_FAIL("Type {} is not supported", typeid(T).name());
        }
    }
};

class RowSerializer : public TypeSerializer<RowVector>{
public:
    RowSerializer(const RowTypePtr& rowType) : rowType_(rowType) {
        
    }
private:
    RowTypePtr rowType_;
};

class MapSerializer {

};

class ArraySerializer {

};
}