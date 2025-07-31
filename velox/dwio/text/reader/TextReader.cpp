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

#include "velox/dwio/text/reader/TextReader.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"

#include <boost/algorithm/string.hpp>
#include <folly/Try.h>
#include <typeinfo>

namespace facebook::velox::text {

TextRowReader::TextRowReader(
    const RowTypePtr& schema,
    const std::shared_ptr<dwio::common::ReadFileInputStream>& fileInput,
    const RowReaderOptions& options) 
: schema_(schema),
options_(options),
fileInput_(fileInput),
fileSize_(fileInput_->getReadFile()->size()) {}

uint64_t TextRowReader::next(uint64_t size, velox::VectorPtr& result, const dwio::common::Mutation*) {
    if (totalReadBytes_ >= fileSize_) {
        readFinished_ = true;
        readRows_ = 0;
        return 0;
    }
    VELOX_CHECK(fileInput_ != nullptr);
    if (result == nullptr) {
        result = RowVector::create(schema_, size, &options_.memoryPool());
    }
    RowVectorPtr row = std::dynamic_pointer_cast<RowVector>(result);
    uint64_t readLength = options_.maxReadSize();
    char* dataToRead;
    fileInput_->read(dataToRead, totalReadBytes_ + 1, readLength, dwio::common::MetricsLog::MetricsType::FILE);
    std::string_view s(dataToRead, readLength);
    std::vector<std::string> lines;
    boost::split(lines, s, boost::is_any_of(lineDelimiter_));
    
    auto readFields = [&](RowVectorPtr& rowVector, const std::string& line, const size_t rowIndex) -> void {
        std::vector<std::string> fields;
        boost::split(fields, line, boost::is_any_of(options_.fieldDemiliter()));
        for (size_t j = 0; j < fields.size(); ++j) {
            deserialize(rowVector->childAt(j), schema_->childAt(j), rowIndex, fields[j]);
        }
    };
    if (lines.size() > 1) {
        for (size_t i = 0; i < lines.size() - 1; ++i) {
            readFields(row, lines[i], i);
            totalReadRows_++;
            totalReadBytes_ += lines[i].size();
        }
        readRows_ = lines.size() - 1;
    } else if (lines.size() == 1) {
        readFields(row, lines[0], 0);
        readRows_ = 1;
    }
    return readRows_;
}

int64_t TextRowReader::nextRowNumber() {
    return options_.maxReadRows();
}

int64_t TextRowReader::nextReadSize(uint64_t size) {
    return readRows_;
}

template<typename T>
const inline T convertTo(const std::string& s, const T& defaultValue, std::optional<std::string>& errMsg) {
    auto result = folly::tryTo<T>(s);
    if (result.hasValue()) {
        return result.value();
    } else {
        std::stringstream ss;
        ss << "Failed to convert:" << s << " to type:" << typeid(T).name();
        errMsg.emplace(ss.str());
        return defaultValue;
    }
}

const void TextRowReader::deserialize(VectorPtr& field, const TypePtr& type, const size_t index, const std::string& s) {
    std::optional<std::string> errMsg;
    switch(type->kind()) {
        case TypeKind::BOOLEAN:
            field->asFlatVector<bool>()->set(index, convertTo<bool>(s, false, errMsg));
            break;
        case TypeKind::TINYINT:
            field->asFlatVector<int8_t>()->set(index, convertTo<int8_t>(s, 0, errMsg));
            break;
        case TypeKind::SMALLINT:
            field->asFlatVector<int16_t>()->set(index, convertTo<int16_t>(s, 0, errMsg));
            break;
        case TypeKind::INTEGER:
            field->asFlatVector<int32_t>()->set(index, convertTo<int32_t>(s, 0, errMsg));
            break;
        case TypeKind::BIGINT:
            field->asFlatVector<int64_t>()->set(index, convertTo<int64_t>(s, 0, errMsg));
            break;
        case TypeKind::REAL:
            field->asFlatVector<float>()->set(index, convertTo<float>(s, 0, errMsg));
            break;
        case TypeKind::DOUBLE:
            field->asFlatVector<double>()->set(index, convertTo<double>(s, 0, errMsg));
            break;
        case TypeKind::VARCHAR: {
            StringView sv(s.data(), s.size());
            field->asFlatVector<StringView>()->set(index, sv);
            break;
        }
        case TypeKind::TIMESTAMP: {
            const auto timestamp = util::fromTimestampString(
                s.data(), s.size(), util::TimestampParseMode::kLegacyCast)
                .thenOrThrow(folly::identity, [&](const Status& status) {
                    VELOX_FAIL("error while parse timestamp: {}", status.message());
                });
            field->asFlatVector<Timestamp>()->set(index, timestamp);
            break;
        }
        default:
            VELOX_UNSUPPORTED("The type of {} is not supported currently.", type->name());
    }
    if (errMsg.has_value()) {
        VELOX_FAIL(errMsg.value());
    }
}

std::optional<size_t> TextRowReader::estimatedRowSize() const {
    VELOX_CHECK(schema_ != nullptr);
    const std::vector<std::shared_ptr<const Type>>& fieldTypes = schema_->children();
    std::optional<size_t> result;
    size_t rowSize = 0;
    for (const auto& fieldType : fieldTypes) {
        VELOX_CHECK(fieldType->isPrimitiveType());
        rowSize += fieldType->cppSizeInBytes();
    }
    return result.emplace(rowSize);
}

std::unique_ptr<dwio::common::RowReader> TextReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
    const RowReaderOptions* rowReaderOptions = reinterpret_cast<const RowReaderOptions*>(&options);
    return std::make_unique<TextRowReader>(
        options_.fileSchema(),
        buffer_->getInputStream(),
        *rowReaderOptions);
}

std::unique_ptr<dwio::common::Reader> TextReaderFcreateReader(
     std::unique_ptr<dwio::common::BufferedInput> buffer,
     const dwio::common::ReaderOptions& options) {
    return std::make_unique<TextReader>(buffer, options);
}

}