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

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/common/memory/MemoryPool.h"
#include "folly/dynamic.h"

namespace facebook::velox::connector::kafka {

using String = std::string;

class KafkaRecordDeserializer {
public:
    KafkaRecordDeserializer(const RowTypePtr & outputType, memory::MemoryPool * memoryPool) 
    : outputType_(outputType), 
    memoryPool_(memoryPool) {
        VELOX_CHECK_GT(outputType_->size(), 0, "Output type size of record deserializer must great than 0.");
        VELOX_CHECK_NOT_NULL(memoryPool_, "Memory pool of record deserializer must not be null.");
    }

    virtual const RowVectorPtr deserialize(const String & message) = 0;

    const RowVectorPtr emptyRow();

protected:
    RowTypePtr outputType_;
    memory::MemoryPool * memoryPool_;

    const time_t convertToTimestamp(const String & val, const String & format);
};

class KafkaJSONRecordDeserializer : public KafkaRecordDeserializer {
public:
    KafkaJSONRecordDeserializer(const RowTypePtr & outputType, memory::MemoryPool * memoryPool) : KafkaRecordDeserializer(outputType, memoryPool) {}

    const RowVectorPtr deserialize(const String & message) override;

private:
    const VectorPtr deserializeField(const folly::dynamic & f, const TypePtr & type);
};

class KafkaCSVRecordDeserializer : public KafkaRecordDeserializer {
public:
    KafkaCSVRecordDeserializer(const RowTypePtr & outputType, memory::MemoryPool * memoryPool) : KafkaRecordDeserializer(outputType, memoryPool) {}

    const RowVectorPtr deserialize(const String & message) override;

};

class KafkaRawRecordDeserializer : public KafkaRecordDeserializer {
public:
    KafkaRawRecordDeserializer(const RowTypePtr & outputType, memory::MemoryPool * memoryPool) : KafkaRecordDeserializer(outputType, memoryPool) {
        VELOX_CHECK_EQ(outputType_->size(), 1, "Output type size of raw deserializer must be 1.");
        const TypePtr & childType = outputType_->childAt(0);
        VELOX_CHECK_EQ(childType->kind(), TypeKind::VARCHAR, "Output type must be Row(String).");
    }

    const RowVectorPtr deserialize(const String & message) override;
};

using KafkaRecordDeserializerPtr = std::shared_ptr<KafkaRecordDeserializer>;

}
