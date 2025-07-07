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
#include "velox/connectors/filesystem/FileSystemConfig.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/text/writer/TextWriter.h"

namespace facebook::velox::connector::filesystem {

using FileSystemWriteConfigPtr = std::shared_ptr<FileSystemWriteConfig>;
using InsertTableHandlePtr = std::shared_ptr<connector::ConnectorInsertTableHandle>;

class FileSystemDataSink : public DataSink {
public:
    FileSystemDataSink(
      const RowTypePtr& inputType,
      const InsertTableHandlePtr& tableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      const FileSystemWriteConfigPtr& writeConfig);

    void appendData(RowVectorPtr input) override;

    bool finish() override;

    std::vector<std::string> close() override;
    
    void abort() override;

    connector::DataSink::Stats stats() const override;
private:
    const RowTypePtr inputType_;
    const ConnectorQueryCtx* queryCtx_;
    const FileSystemWriteConfigPtr writeConfig_;
    std::unique_ptr<text::TextWriter> writer_;

    const std::unique_ptr<text::TextWriter> createWriter();
};

}