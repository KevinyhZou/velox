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
#include "velox/connectors/filesystem/FileSystemIndexTable.h"
#include <fmt/format.h>

namespace facebook::velox::connector::filesystem {

class FileSystemIndexTableHandle : public connector::ConnectorTableHandle {
 public:
  FileSystemIndexTableHandle(
      std::string connectorId,
      std::shared_ptr<FileSystemIndexTable> indexTable,
      bool asyncLookup)
      : ConnectorTableHandle(std::move(connectorId)),
        indexTable_(std::move(indexTable)),
        asyncLookup_(asyncLookup) {}

  ~FileSystemIndexTableHandle() override = default;

  std::string toString() const override {
    return fmt::format(
        "IndexTableHandle: num of rows: {}, asyncLookup: {}",
        indexTable_ ? indexTable_->table->rows()->numRows() : 0,
        asyncLookup_);
  }

  const std::string& name() const override {
    static const std::string kTableHandleName{"TestIndexTableHandle"};
    return kTableHandleName;
  }

  bool supportsIndexLookup() const override {
    return true;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = name();
    obj["connectorId"] = connectorId();
    obj["asyncLookup"] = asyncLookup_;
    return obj;
  }

  static std::shared_ptr<FileSystemIndexTableHandle> create(
      const folly::dynamic& obj,
      void* context) {
    // NOTE: this is only for testing purpose so we don't support to serialize
    // the table.
    return std::make_shared<FileSystemIndexTableHandle>(
        obj["connectorId"].getString(), nullptr, obj["asyncLookup"].asBool());
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("TestIndexTableHandle", create);
  }

  const std::shared_ptr<FileSystemIndexTable>& indexTable() const {
    return indexTable_;
  }

  /// If true, we returns the lookup result asynchronously for testing purpose.
  bool asyncLookup() const {
    return asyncLookup_;
  }

 private:
  const std::shared_ptr<FileSystemIndexTable> indexTable_;
  const bool asyncLookup_;
};

}
