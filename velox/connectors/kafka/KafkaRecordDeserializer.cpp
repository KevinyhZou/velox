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

#include "velox/connectors/kafka/KafkaRecordDeserializer.h"
#include "velox/type/Type.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "folly/json/json.h"
#include <ctime>
#include <chrono>

namespace facebook::velox::connector::kafka {

    const time_t KafkaRecordDeserializer::convertToTimestamp(const String & val, const String & format) {
        std::tm tm = {};
        std::istringstream ss(val);
        ss >> std::get_time(&tm, format.c_str());
        if (ss.fail()) {
           VELOX_FAIL("Failed to parse timestamp, with value:{}, format:{}", val, format);
        }
        time_t timestamp = std::mktime(&tm);
        return timestamp;
    }

    const RowVectorPtr KafkaRecordDeserializer::emptyRow() {
        return RowVector::createEmpty(outputType_, memoryPool_);
    }

    const VectorPtr KafkaJSONRecordDeserializer::deserializeField(const folly::dynamic & f, const TypePtr & fieldType) {
        VectorPtr fieldVector = nullptr;
        const TypeKind kind = fieldType->kind();
        if (fieldType->isPrimitiveType()) {
            fieldVector = BaseVector::create(fieldType, 1, memoryPool_);
            if (f.isNull()) {
                fieldVector->setNull(0, true);
                return fieldVector;
            } else if (kind == TypeKind::INTEGER) {
                auto flat = std::dynamic_pointer_cast<FlatVector<int32_t>>(fieldVector);
                flat->set(0, static_cast<int32_t>(f.asInt()));
            } else if (kind == TypeKind::BIGINT) {
                auto flat = std::dynamic_pointer_cast<FlatVector<int64_t>>(fieldVector);
                flat->set(0, static_cast<int64_t>(f.asInt()));
            } else if (kind == TypeKind::HUGEINT) {
                auto flat = std::dynamic_pointer_cast<FlatVector<int128_t>>(fieldVector);
                flat->set(0, static_cast<int128_t>(f.asInt()));
            } else if (kind == TypeKind::BOOLEAN) {
                auto flat = std::dynamic_pointer_cast<FlatVector<bool>>(fieldVector);
                flat->set(0, f.asBool());
            } else if (kind == TypeKind::REAL) {
                auto flat = std::dynamic_pointer_cast<FlatVector<float>>(fieldVector);
                flat->set(0, static_cast<float>(f.asDouble()));
            } else if (kind == TypeKind::DOUBLE) {
                auto flat = std::dynamic_pointer_cast<FlatVector<double>>(fieldVector);
                flat->set(0, f.asDouble());
            } else if (kind == TypeKind::SMALLINT) {
                auto flat = std::dynamic_pointer_cast<FlatVector<int16_t>>(fieldVector);
                flat->set(0, static_cast<int16_t>(f.asInt()));
            } else if (kind == TypeKind::TINYINT) {
                auto flat = std::dynamic_pointer_cast<FlatVector<int8_t>>(fieldVector);
                flat->set(0, static_cast<int8_t>(f.asInt()));
            } else if (kind == TypeKind::TIMESTAMP) {
                auto flat = std::dynamic_pointer_cast<FlatVector<Timestamp>>(fieldVector);
                if (f.isString()) {
                    const time_t timestampVal = convertToTimestamp(f.asString(), "%Y-%m-%d %H:%M:%S");
                    flat->set(0, Timestamp::fromMillis(timestampVal));
                } else if (f.isInt()) {
                    flat->set(0, Timestamp::fromMillis(f.asInt()));
                } else {
                    VELOX_FAIL("Failed to convert folly::dynamic:{} to Tiimestamp, which is not string or int type.", f.typeName());
                }
            } else if (kind == TypeKind::VARBINARY || kind == TypeKind::VARCHAR) {
                auto flat = std::dynamic_pointer_cast<FlatVector<StringView>>(fieldVector); 
                const String s = f.asString();
                const StringView data(s.data(), s.size());
                flat->set(0, data);
            } else {
                VELOX_FAIL("Failed to convert folly::dynamic to row, as not supported the field type:{}", fieldType->name());
            }
        } else {
            if (kind == TypeKind::ROW) {
                const RowTypePtr rowType = std::dynamic_pointer_cast<const RowType>(fieldType);
                const std::vector<String> & subRowFieldNames = rowType->names();
                const std::vector<TypePtr> & subRowFieldTypes = rowType->children();
                std::vector<VectorPtr> subRowFields;
                for (size_t i = 0; i < subRowFieldNames.size(); ++i) {
                    const VectorPtr subRowField = deserializeField(f.isNull() ? nullptr : f[subRowFieldNames[i]], subRowFieldTypes[i]);
                    subRowFields.emplace_back(subRowField);
                }
                fieldVector = std::make_shared<RowVector>(memoryPool_, rowType, BufferPtr(nullptr), 1, subRowFields);
            } else if (kind == TypeKind::ARRAY) {
                const std::shared_ptr<const ArrayType> arrayType = std::dynamic_pointer_cast<const ArrayType>(fieldType);
                const TypePtr & elementType = arrayType->elementType();
                VectorPtr elements = BaseVector::create(elementType, 0, memoryPool_);
                for (size_t i = 0; i < f.size(); ++i) {
                    const VectorPtr element = deserializeField(f.isNull() ? nullptr : f[i], elementType);
                    elements->append(element.get());
                }
                fieldVector = std::make_shared<ArrayVector>(memoryPool_, arrayType, BufferPtr(nullptr), 1,
                    allocateOffsets(f.size(), memoryPool_), allocateSizes(f.size(), memoryPool_), elements);
            } else if (kind == TypeKind::MAP) {
                const std::shared_ptr<const MapType> mapType = std::dynamic_pointer_cast<const MapType>(fieldType);
                const TypePtr & keyType = mapType->keyType();
                const TypePtr & valueType = mapType->valueType();
                VectorPtr keys = BaseVector::create(keyType, 0, memoryPool_);
                VectorPtr values = BaseVector::create(valueType, 0, memoryPool_);
                for (const auto & pair : f.items()) {
                    const VectorPtr key = deserializeField(f.isNull() ? nullptr : pair.first, keyType);
                    const VectorPtr value = deserializeField(f.isNull() ? nullptr : pair.second, valueType);
                    keys->append(key.get());
                    values->append(value.get());
                }
                fieldVector = std::make_shared<MapVector>(memoryPool_, mapType, BufferPtr(nullptr), 1,
                    allocateOffsets(f.size(), memoryPool_), allocateSizes(f.size(), memoryPool_), keys, values);
            } else {
                VELOX_FAIL("Failed to convert folly::dynamic to row, as not supported the field type:{}", fieldType->name());
            }
            if (f.isNull()) {
                fieldVector->setNull(0, true);
            }
        }
        return fieldVector;
    }

    const RowVectorPtr KafkaJSONRecordDeserializer::deserialize(const String & msg) {
        try {
            folly::dynamic obj = folly::parseJson(msg);
            const std::vector<String> & fieldNames = outputType_->names();
            const std::vector<TypePtr> & fieldTypes = outputType_->children();
            std::vector<VectorPtr> fieldVectors;
            for (size_t i = 0; i < fieldNames.size(); ++i) {
                const VectorPtr fieldVector = deserializeField(obj[fieldNames[i]], fieldTypes[i]);
                fieldVectors.emplace_back(fieldVector);
            }
            return std::make_shared<RowVector>(memoryPool_, outputType_, BufferPtr(nullptr), 1, fieldVectors);
        } catch (const std::exception & e) {
            LOG(WARNING) << "Failed to deserialize record: " << msg <<" , error: " << e.what();
            return emptyRow();
        }
    }

    const RowVectorPtr KafkaCSVRecordDeserializer::deserialize(const String & message) {
        return nullptr;
    }

    const RowVectorPtr KafkaRawRecordDeserializer::deserialize(const String & message) {
        std::vector<VectorPtr> children;
        VectorPtr child = BaseVector::create(outputType_->childAt(0), 1, memoryPool_);
        child->resize(1);
        auto flat = std::dynamic_pointer_cast<FlatVector<velox::StringView>>(child);
        const StringView s(message.data(), message.size());
        flat->set(0, s);
        children.emplace_back(child);
        return std::make_shared<RowVector>(memoryPool_, outputType_, BufferPtr(nullptr), 1, children);
    }
 }
 
