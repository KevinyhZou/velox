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

#include "velox/common/compression/Compression.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorStream.h"
#include "velox/type/Timestamp.h"
#include "velox/type/StringView.h"
#include "velox/type/HugeInt.h"
#include "velox/serializers/PrestoSerializer.h"
#include "rocksdb/slice.h"
#include <cstdint>
#include <memory>
#include <type_traits>
#include <typeinfo>

namespace facebook::velox::stateful {

class TypeBaseSerializer {};

using TypeSerializerPtr = std::shared_ptr<TypeBaseSerializer>;

template<typename D, typename B>
class TypeSerializer : public TypeBaseSerializer {
public:
    /// Serialize the given value to char array.
    virtual const B serialize(const D& data)  = 0;

    /// Deserialize the give char array to value.
    virtual const D deserialize(const B& bytes) = 0;

protected:
    std::unique_ptr<ByteInputStream> toByteStream(const char* data, const size_t len) {
        // ByteRange byteRange{reinterpret_cast<uint8_t*>(const_cast<char*>(data)),(int32_t)len,0};
        // return std::make_unique<BufferInputStream>(std::vector<ByteRange>{{byteRange}});
        return nullptr;
    }

    const B convertToBytesOutput(const char* data, const size_t len) {
        if constexpr (std::is_same_v<B, rocksdb::Slice>) {
            return rocksdb::Slice(data, len);
        } else if constexpr (std::is_same_v<B, facebook::velox::StringView>) {
            return facebook::velox::StringView(data, len);
        } else {
            VELOX_FAIL("Not support output type: {} for type serializer", typeid(B).name());
        }
    }

    const std::pair<const char*, const size_t> convertToBytesInput(const B bytes) {
        if constexpr (std::is_same_v<B, rocksdb::Slice>) {
            return std::pair<const char*, const size_t>(bytes.data(), bytes.size());
        } else if constexpr (std::is_same_v<B, facebook::velox::StringView>) {
            return std::pair<const char*, const size_t>(bytes.data(), bytes.size());
        } else {
            VELOX_FAIL("Not support output type: {} for type serializer", typeid(B).name());
        }
    }

    VectorSerde::Options getDefaultCompressionOptions() {
        VectorSerde::Options opts(common::CompressionKind::CompressionKind_SNAPPY, 0.8);
        return opts;
    }
};

template<typename D, typename B>
class ValueSerializer : public TypeSerializer<D, B> {
public:
    const B serialize(const D& t) override {
        if constexpr (
            std::is_same_v<D, int8_t> || std::is_same_v<D, int16_t> ||
            std::is_same_v<D, int32_t> || std::is_same_v<D, int64_t> ||
            std::is_same_v<D, int128_t> || std::is_same_v<D, uint8_t> ||
            std::is_same_v<D, uint16_t> || std::is_same_v<D, uint32_t> ||
            std::is_same_v<D, uint64_t> || std::is_same_v<D, uint128_t> ||
            std::is_same_v<D, float> || std::is_same_v<D, double>) {
            char chs[sizeof(D)];
            std::memcpy(chs, &t, sizeof(D));
            return TypeSerializer<D, B>::convertToBytesOutput(chs, sizeof(D));
        } else if constexpr (std::is_same_v<D, bool>) {
            uint8_t byteValue = t ? 1 : 0;
            char chs[sizeof(uint8_t)];
            std::memcpy(chs, &byteValue, sizeof(uint8_t));
            return TypeSerializer<D, B>::convertToBytesOutput(chs, sizeof(uint8_t));
        } else if constexpr (std::is_same_v<D, StringView>) {
            return TypeSerializer<D, B>::convertToBytesOutput(t.data(), t.size());
        } else if constexpr (std::is_same_v<D, Timestamp>) {
            int64_t mills = t.toMillis();
            char chs[sizeof(int64_t)];
            std::memcpy(chs, &t, sizeof(int64_t));
            return TypeSerializer<D, B>::convertToBytesOutput(chs, sizeof(int64_t));
        } else {
            VELOX_FAIL("Type {} is not supported", typeid(D).name());
        }
    }

    const D deserialize(const B& bytes) override {
        std::pair<const char*, const size_t> inputBytes = TypeSerializer<D, B>::convertToBytesInput(bytes);
        if constexpr (
            std::is_same_v<D, int8_t> || std::is_same_v<D, int16_t> ||
            std::is_same_v<D, int32_t> || std::is_same_v<D, int64_t> ||
            std::is_same_v<D, int128_t> || std::is_same_v<D, uint8_t> ||
            std::is_same_v<D, uint16_t> || std::is_same_v<D, uint32_t> ||
            std::is_same_v<D, uint64_t> || std::is_same_v<D, uint128_t> ||
            std::is_same_v<D, float> || std::is_same_v<D, double>) {
            D t;
            std::memcpy(&t, inputBytes.first, inputBytes.second);
            return t;
        } else if constexpr (std::is_same_v<D, bool>) {
            uint8_t byteValue;
            std::memcpy(&byteValue, inputBytes.first, inputBytes.second);
            return byteValue > 0 ? true : false;
        } else if constexpr (std::is_same_v<D, StringView>) {
            return StringView(inputBytes.first, inputBytes.second);
        } else if constexpr (std::is_same_v<D, Timestamp>) {
            int64_t mills;
            std::memcpy(&mills, inputBytes.first, inputBytes.second);
            return Timestamp::fromMillis(mills);
        } else {
            VELOX_FAIL("Type {} is not supported", typeid(D).name());
        }
    }
};

template<typename D, typename B>
class ComplexVectorSerializer : public TypeSerializer<D, B>{
public:
    ComplexVectorSerializer(const TypePtr& dataType, memory::MemoryPool* pool) 
    : dataType_(dataType), pool_(pool), serde_(std::make_shared<serializer::presto::PrestoVectorSerde>()) {
        checkTypes();
    }

    const B serialize(const D& t) override {
        // const size_t dataBytes = t->inMemoryBytes();
        // char chs[dataBytes];
        // auto byteStream = TypeSerializer<D, B>::toByteStream(chs, dataBytes);
        // serde_->serializeSingleColumn(
        //     t, 
        //     &TypeSerializer<D, B>::getDefaultCompressionOptions(),
        //     pool_,
        //     nullptr); // byteStream.get());
        // return TypeSerializer<D, B>::convertToBytesOutput(chs, dataBytes);
        VELOX_FAIL("1111");
    }
    
    const D deserialize(const B& bytes) override { 
        // const std::pair<const char*, const size_t> inputBytes = TypeSerializer<D, B>::convertToBytesInput(bytes);
        // auto byteStream = TypeSerializer<D, B>::toByteStream(inputBytes.first, inputBytes.second);
        // D result;
        // serde_->deserializeSingleColumn(
        //     nullptr,//byteStream.get(), 
        //     pool_, 
        //     dataType_, 
        //     &result, 
        //     &TypeSerializer<D, B>::getDefaultCompressionOptions());
        // return result; 
        VELOX_FAIL("2222");
    }

private:
    const TypePtr dataType_;
    const memory::MemoryPool* pool_;
    const std::shared_ptr<serializer::presto::PrestoVectorSerde> serde_;

    void checkTypes() {
        if (!std::is_same_v<D, RowVectorPtr>
            && !std::is_same_v<D, ArrayVectorPtr>
            && !std::is_same_v<D, MapVectorPtr>) {
            VELOX_FAIL("Vector type not valid, this complex vector seralizer can only suupport rowvector/arrayvector/mapvector.");
        }
        if (!std::is_same_v<B, rocksdb::Slice>
            && !std::is_same_v<B, StringView>) {
            VELOX_FAIL("Output Bytes type not valid, thie complex vector serializer can not support rocksdb::slice/stringview.");
        }
    }
};

template<typename B>
TypeSerializerPtr createSerializer(const TypePtr& type, memory::MemoryPool* pool = nullptr) {
    const TypeKind kind = type->kind();
    if(kind == TypeKind::INTEGER) {
        using T0 = TypeTraits<TypeKind::INTEGER>::NativeType;
        return std::make_shared<ValueSerializer<T0, B>>();
    } else if (kind == TypeKind::BIGINT) {
        using T1 = TypeTraits<TypeKind::BIGINT>::NativeType;
        return std::make_shared<ValueSerializer<T1, B>>();
    } else if (kind == TypeKind::HUGEINT) {
        using T2 = TypeTraits<TypeKind::HUGEINT>::NativeType;
        return std::make_shared<ValueSerializer<T2, B>>();
    } else if (kind == TypeKind::SMALLINT) {
        using T3 = TypeTraits<TypeKind::SMALLINT>::NativeType;
        return std::make_shared<ValueSerializer<T3, B>>();
    } else if (kind == TypeKind::TINYINT) {
        using T4 = TypeTraits<TypeKind::TINYINT>::NativeType;
        return std::make_shared<ValueSerializer<T4, B>>();
    } else if (kind == TypeKind::REAL) {
        using T5 = TypeTraits<TypeKind::REAL>::NativeType;
        return std::make_shared<ValueSerializer<T5, B>>();
    } else if (kind == TypeKind::DOUBLE) {
        using T6 = TypeTraits<TypeKind::DOUBLE>::NativeType;
        return std::make_shared<ValueSerializer<T6, B>>();
    } else if (kind == TypeKind::BOOLEAN) {
        using T7 = TypeTraits<TypeKind::BOOLEAN>::NativeType;
        return std::make_shared<ValueSerializer<T7, B>>();
    } else if (kind == TypeKind::VARCHAR) {
        using T8 = TypeTraits<TypeKind::VARCHAR>::NativeType;
        return std::make_shared<ValueSerializer<T8, B>>();
    } else if (kind == TypeKind::TIMESTAMP) {
        using T9 = TypeTraits<TypeKind::TIMESTAMP>::NativeType;
        return std::make_shared<ValueSerializer<T9, B>>();
    } else if (kind == TypeKind::ROW) {
        const std::shared_ptr<const RowType> rowType = std::dynamic_pointer_cast<const RowType>(type);
        return std::make_shared<ComplexVectorSerializer<RowVectorPtr, B>>(rowType, pool);
    } else if (kind == TypeKind::ARRAY) {
        const std::shared_ptr<const ArrayType> arrayType = std::dynamic_pointer_cast<const ArrayType>(type);
        return std::make_shared<ComplexVectorSerializer<ArrayVectorPtr, B>>(arrayType, pool);
    } else if (kind == TypeKind::MAP) {
        const std::shared_ptr<const MapType> mapType = std::dynamic_pointer_cast<const MapType>(type);
        return std::make_shared<ComplexVectorSerializer<MapVectorPtr, B>>(mapType, pool);
    } else {
        VELOX_FAIL("Type {} not supported", type->name());
    }
}
}
