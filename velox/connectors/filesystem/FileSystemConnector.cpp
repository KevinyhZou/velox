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

#include "velox/connectors/filesystem/FileSystemConnector.h"

namespace facebook::velox::connector::filesystem {

    std::unique_ptr<DataSource> FileSystemConnector::createDataSource(
        const RowTypePtr& outputType,
        const ConnectorHandlePtr& tableHandle,
        const std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>& columnHandles,
        ConnectorQueryCtx* connectorQueryCtx) {
        
        VELOX_NYI();
    }

    std::unique_ptr<DataSink> FileSystemConnector::createDataSink(
        RowTypePtr inputType,
        ConnectorInsertTableHandlePtr connectorInsertTableHandle,
        ConnectorQueryCtx* connectorQueryCtx,
        CommitStrategy /** commitStrategy */) {
        
        std::shared_ptr<FileSystemWriteConfig> writeConfig = std::make_shared<FileSystemWriteConfig>(config_);
        return std::make_unique<FileSystemDataSink>(
            inputType,
            connectorInsertTableHandle,
            connectorQueryCtx,
            writeConfig);
    }
}