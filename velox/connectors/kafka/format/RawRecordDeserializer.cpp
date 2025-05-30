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

#include "velox/connectors/kafka/format/RawRecordDeserializer.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::kafka {

const void KafkaRawRecordDeserializer::deserialize(const std::string & message, const size_t index, VectorPtr & vec) {
    RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    VELOX_CHECK_EQ(rowVector->childrenSize(), 1, "The raw record vector children size {} is not 1", rowVector->childrenSize());
    VectorPtr & childVector = rowVector->children()[0];
    auto flat = std::dynamic_pointer_cast<FlatVector<velox::StringView>>(childVector);
    flat->set(0, StringView(message.data(), message.size()));
}

const void KafkaRawRecordDeserializer::deserialize(const std::vector<std::string> & messages, VectorPtr & vec) {
    RowVectorPtr rowVector = std::dynamic_pointer_cast<RowVector>(vec);
    VELOX_CHECK_EQ(rowVector->childrenSize(), 1, "The raw record vector children size {} is not 1", rowVector->childrenSize());
    VectorPtr & childVector = rowVector->children()[0];
    auto flat = std::dynamic_pointer_cast<FlatVector<velox::StringView>>(childVector);
    for (size_t i = 0; i < messages.size(); ++i) {
        flat->set(i, StringView(messages[i].data(), messages[i].size()));
    }
}

}
