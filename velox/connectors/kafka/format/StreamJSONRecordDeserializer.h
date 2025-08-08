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

#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"
#include "velox/connectors/kafka/KafkaRecordDeserializer.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include <type_traits>
#include <typeinfo>

namespace facebook::velox::connector::kafka {

using JSONDoc = simdjson::ondemand::document;
using JSONValue = simdjson::ondemand::value;
using JSONRow = simdjson::ondemand::object;
using JSONArray = simdjson::ondemand::array;
using JSONMap = simdjson::ondemand::object;

struct StreamJSONDeserializer {
 public:
  static const std::shared_ptr<StreamJSONDeserializer> create(
      const TypePtr& type);

  inline virtual const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) = 0;
};

struct StreamIntDeserializer : public StreamJSONDeserializer {
 public:
  StreamIntDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<int32_t>>(vec);
    flat->set(index, static_cast<int32_t>(e.get_int64()));
  }
};

struct StreamBigIntDeserializer : public StreamJSONDeserializer {
 public:
  StreamBigIntDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec);
    flat->set(index, e.get_int64());
  }
};

struct StreamHugeIntDeserializer : public StreamJSONDeserializer {
 public:
  StreamHugeIntDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<int128_t>>(vec);
    flat->set(index, static_cast<int128_t>(e.get_int64()));
  }
};

struct StreamBoolDeserializer : public StreamJSONDeserializer {
  public:
    StreamBoolDeserializer() {}
  
  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<bool>>(vec);
    flat->set(index, e.get_bool());
  }
};

struct StreamFloatDeserializer : public StreamJSONDeserializer {
public:
  StreamFloatDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<float>>(vec);
    flat->set(index, static_cast<float>(e.get_double()));
  }

};

struct StreamDoubleDeserializer : public StreamJSONDeserializer {
  public:
    StreamDoubleDeserializer() {}
  
  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat = std::dynamic_pointer_cast<FlatVector<double>>(vec);
    flat->set(index, e.get_double());
  }
};

struct StreamStringDeserializer : public StreamJSONDeserializer {
 public:
  StreamStringDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat =
        std::dynamic_pointer_cast<FlatVector<facebook::velox::StringView>>(vec);
    std::string_view s = e.get_string();
    flat->set(index, facebook::velox::StringView(s.data(), s.size()));
  }
};

struct StreamTimestampDeserializer : public StreamJSONDeserializer {
 public:
  StreamTimestampDeserializer() {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    auto flat =
        std::dynamic_pointer_cast<FlatVector<facebook::velox::Timestamp>>(vec);
    std::string_view s = e.get_string();
    const auto timestamp =
        util::fromTimestampString(
            s.data(), s.size(), util::TimestampParseMode::kLegacyCast)
            .thenOrThrow(folly::identity, [&](const Status& status) {
              VELOX_FAIL("error while parse timestamp: {}", status.message());
            });
    flat->set(index, timestamp);
  }
};

struct StreamRowDeserializer : public StreamJSONDeserializer {
 public:
  StreamRowDeserializer(
      const std::vector<std::string>& fieldNames,
      const std::vector<TypePtr>& fieldTypes,
      const std::vector<std::shared_ptr<StreamJSONDeserializer>>& deserializers)
      : fieldNames_(fieldNames),
        fieldTypes_(fieldTypes),
        deserializers_(deserializers) {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    std::vector<VectorPtr>& rowFields = rowVector->children();
    JSONRow row = e.get_object();
    for (size_t i = 0; i < fieldNames_.size(); ++i) {
      JSONValue v;
      auto err = row[fieldNames_[i]].get(v);
      if (err != simdjson::error_code::SUCCESS || v.is_null()) {
        rowFields[i]->setNull(index, true);
      } else {
        deserializers_[i]->deserialize(v, index, rowFields[i]);
      }
    }
  }

 private:
  std::vector<std::string> fieldNames_;
  std::vector<TypePtr> fieldTypes_;
  std::vector<std::shared_ptr<StreamJSONDeserializer>> deserializers_;
};

struct StreamArrayDeserializer : public StreamJSONDeserializer {
  public:
    StreamArrayDeserializer(
      const TypePtr& elementType,
      const std::shared_ptr<StreamJSONDeserializer>& deserializer
    ): elementType_(elementType), deserializer_(deserializer) {}
  
  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    std::shared_ptr<ArrayVector> arrayVector = std::dynamic_pointer_cast<ArrayVector>(vec);
    auto offset = arrayVector->offsetAt(index);
    VectorPtr& elements = arrayVector->elements();
    JSONArray array = e.get_array();
    for (size_t i = 0; i < array.count_elements(); ++i) {
      auto v = array.at(i);
      if (v.error() != simdjson::error_code::SUCCESS || v.value().is_null()) {
        elements->setNull(offset + i, true);
      } else {
        deserializer_->deserialize(v.value(), offset + i, elements);
      }
    }
  }

  private:
    const TypePtr elementType_;
    const std::shared_ptr<StreamJSONDeserializer> deserializer_; 
};

struct StreamMapDeserializer : public StreamJSONDeserializer {
  public:
    StreamMapDeserializer(
      const TypePtr& keyType,
      const TypePtr& valueType,
      const std::shared_ptr<StreamJSONDeserializer>& keyDeserializer,
      const std::shared_ptr<StreamJSONDeserializer>& valueDeserializer
    ): keyType_(keyType),
    valueType_(valueType),
    keyDeserializer_(keyDeserializer),
    valueDeserializer_(valueDeserializer) {}

  inline const void
  deserialize(JSONValue& e, const size_t index, VectorPtr& vec) override {
    std::shared_ptr<MapVector> mapVector = std::dynamic_pointer_cast<MapVector>(vec);
    auto offset = mapVector->offsetAt(index);
    auto size = mapVector->sizeAt(index);
    auto& keys = mapVector->mapKeys();
    auto& values = mapVector->mapValues();
    auto flatKeys = std::dynamic_pointer_cast<FlatVector<StringView>>(keys);

    JSONMap map = e.get_object();
    size_t entryIndex = 0;
    for (auto field : map) {
      auto key = field.escaped_key();
      auto val = field.value();
      if (key.error() != simdjson::error_code::SUCCESS) {
        keys->setNull(offset + entryIndex, true);
      } else {
        std::string_view keyStr = key.value();
        flatKeys->set(offset + entryIndex, StringView(keyStr.data(), keyStr.size()));
      }
      if (val.error() != simdjson::error_code::SUCCESS || val.value().is_null()) {
        values->setNull(offset + entryIndex, true);
      } else {
        valueDeserializer_->deserialize(val.value(), offset + entryIndex, values);
      }
      entryIndex ++;
    }
  }

  private:
    const TypePtr& keyType_;
    const TypePtr& valueType_;
    const std::shared_ptr<StreamJSONDeserializer> keyDeserializer_;
    const std::shared_ptr<StreamJSONDeserializer> valueDeserializer_; 
};

/// Class for kafka record deserialization of json format, using the streaming
/// interface of simdjson
class KafkaStreamJSONRecordDeserializer : public KafkaRecordDeserializer {
 public:
  KafkaStreamJSONRecordDeserializer(
      const RowTypePtr& outputType,
      memory::MemoryPool* memoryPool)
      : KafkaRecordDeserializer(outputType, memoryPool),
        deserializer_(std::dynamic_pointer_cast<StreamRowDeserializer>(
            StreamJSONDeserializer::create(outputType))),
        parser_(std::make_shared<simdjson::ondemand::parser>()) {}

  const void deserialize(
      const std::string& message,
      const size_t index,
      VectorPtr& vec) override;

 private:
  std::shared_ptr<StreamRowDeserializer> deserializer_;
  std::shared_ptr<simdjson::ondemand::parser> parser_;
};

} // namespace facebook::velox::connector::kafka
