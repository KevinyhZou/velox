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

#include "velox/connectors/Connector.h"
#include "velox/type/Type.h"
#include "velox/type/Timestamp.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/dwio/common/Writer.h"
#include <sstream>
#include <iostream>
#include <typeinfo>

namespace facebook::velox::connector::print {

struct StringConverter {
public:
    virtual const void convert(const VectorPtr& input, const TypePtr&, const size_t index, std::stringstream& ss) = 0;
};

using ConverterPtr = std::shared_ptr<StringConverter>;

template<typename T>
struct BaseConverter : public StringConverter {
public:
    const void convert(const VectorPtr& input, const TypePtr&, const size_t index, std::stringstream& ss) override {
        auto flat = std::dynamic_pointer_cast<FlatVector<T>>(input);
        if (flat->isNullAt(index)) {
            ss << "null";
        } else {
            convert(flat->valueAt(index), ss);
        }
    }
protected:
    inline const void convert(const T& t, std::stringstream& ss) {
        if constexpr (
            std::is_same_v<T, float> || 
            std::is_same_v<T, double> ||
            std::is_same_v<T, int8_t> ||
            std::is_same_v<T, int16_t> ||
            std::is_same_v<T, int32_t> ||
            std::is_same_v<T, int64_t>) {
            ss << t;
        } else if constexpr (std::is_same_v<T, bool>) {
            std::string s = t ? "true" : "false";
            ss << s;
        } else if constexpr (std::is_same_v<T, StringView>) {
            ss << t.str();
        } else if constexpr (std::is_same_v<T, Timestamp>) {
            ss << t.toString();
        } else {
            VELOX_FAIL("Not support type: {}", typeid(T).name());
        }
    }
};

struct RowStringConverter : public StringConverter {
public:
    RowStringConverter(const std::vector<ConverterPtr>& converters)
    : converters_(converters){}

    const void convert(const VectorPtr& input, const TypePtr& type, const size_t index, std::stringstream& ss) override {
        auto rowInput = std::dynamic_pointer_cast<const RowVector>(input);
        auto rowType = std::dynamic_pointer_cast<const RowType>(type);
        VELOX_CHECK_EQ(rowInput->childrenSize(), rowType->children().size());
        VELOX_CHECK_EQ(rowInput->childrenSize(), converters_.size());
        ss << "+I[";
        for (size_t i = 0; i < rowInput->childrenSize(); ++i) {
            const VectorPtr& field = rowInput->childAt(i);
            const TypePtr& fieldType = rowType->childAt(i);
            if (field->isNullAt(index)) {
                ss << "null";
            } else {
                converters_[i]->convert(field, fieldType, index, ss);
            }
            if (i != rowInput->childrenSize() - 1) {
                ss << ", ";
            }
        }
        ss << "]";
    }

private:
    std::vector<ConverterPtr> converters_; 
};

struct ArrayStringConverter : public StringConverter {
public:
    ArrayStringConverter(const ConverterPtr& elementConverter) 
    : elementConverter_(elementConverter) {}

    const void convert(const VectorPtr& input, const TypePtr& type, const size_t index, std::stringstream& ss) override {
        auto arrayInput = std::dynamic_pointer_cast<const ArrayVector>(input);
        auto arrayType = std::dynamic_pointer_cast<const ArrayType>(type);
        ss << "[";
        auto offset = arrayInput->offsetAt(index);
        auto size = arrayInput->sizeAt(index);
        auto elements = arrayInput->elements();
        for (size_t j = 0; j < size; ++j) {
            auto elementIndex = offset + j;
            if (elements->isNullAt(elementIndex)) {
                ss << "null";
            } else {
                convert(elements, arrayType->elementType(), elementIndex, ss);
            }
            if (j != size - 1) {
                ss << ", ";
            }
        }
        ss << "]";
    }
private:
    ConverterPtr elementConverter_;
};

struct MapStringConverter : public StringConverter {
public:
    MapStringConverter(const ConverterPtr& keyConverter, const ConverterPtr& valueConverter) 
    : keyConverter_(keyConverter), valueConverter_(valueConverter) {}

    const void convert(const VectorPtr& input, const TypePtr& type, const size_t index, std::stringstream& ss) override {
        auto mapInput = std::dynamic_pointer_cast<const MapVector>(input);
        auto mapType = std::dynamic_pointer_cast<const MapType>(type);
        ss << "{";
        auto offset = mapInput->offsetAt(index);
        auto size = mapInput->sizeAt(index);

        auto keys = mapInput->mapKeys();
        auto values = mapInput->mapValues();
        auto keyType = mapType->keyType();
        auto valueType = mapType->valueType();

        for (size_t j = 0; j < size; ++j) {
            auto entryIndex = offset + j;

            if (keys->isNullAt(entryIndex)) {
                ss << "null";
            } else {
                convert(keys, keyType, entryIndex, ss);
            }

            ss << "=";

            if (values->isNullAt(entryIndex)) {
                ss << "null";
            } else {
                convert(values, valueType, entryIndex, ss);
            }

            if (j != size - 1) {
                ss << ", ";
            }
        }
        ss << "}";
    }
private:
    ConverterPtr keyConverter_;
    ConverterPtr valueConverter_;
};

class PrintSink : public DataSink {
public:
    PrintSink(
        const RowTypePtr& inputType,
        const std::string& path,
        const ConnectorQueryCtx* queryCtx
    );

    void appendData(RowVectorPtr input) override;

    bool finish() override;

    std::vector<std::string> close() override;

    void abort() override;

    connector::DataSink::Stats stats() const override;

private:
    const RowTypePtr inputType_;
    const RowTypePtr outputType_;
    const ConnectorQueryCtx* queryCtx_;
    const std::unique_ptr<dwio::common::Writer> writer_;
    const std::shared_ptr<StringConverter> converter_;
    bool finished = true;

    std::unique_ptr<dwio::common::Writer> createWriter(const std::string& path);
    const RowTypePtr createOutputType();
    const RowVectorPtr convertInputToOutput(const RowVectorPtr& input);
    const std::shared_ptr<StringConverter> createStringConverter(const TypePtr& type);
};

}