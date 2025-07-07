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

#include "velox/connectors/filesystem/FileSystemDataSink.h"
#include "velox/dwio/common/FileSink.h"

namespace facebook::velox::connector::filesystem {

FileSystemDataSink::FileSystemDataSink(
      const RowTypePtr& inputType,
      const InsertTableHandlePtr& tableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      const FileSystemWriteConfigPtr& writeConfig) :
    inputType_(inputType),
    queryCtx_(connectorQueryCtx),
    writeConfig_(writeConfig),
    writer_(createWriter()) {}

const std::unique_ptr<text::TextWriter> FileSystemDataSink::createWriter() {
    VELOX_CHECK(writeConfig_->exists(FileSystemWriteConfig::kPath), "Failed create writer because the write path is not specified.");
    VELOX_CHECK(writeConfig_->exists(FileSystemWriteConfig::kFormat), "Failed create write because the write format is not specified");
    const std::string path = writeConfig_->getPath();
    const std::string format = writeConfig_->getFormat();
    if (path.substr(0, 7) == "file://" && format == "csv") {
        dwio::common::FileSink::Options sinkOptions{};
        VELOX_CHECK(inputType_ != nullptr, "Failed to create text writer because the input type is null.");
        std::unique_ptr<dwio::common::FileSink> fileSink = dwio::common::FileSink::create(path, sinkOptions);
        return std::make_unique<text::TextWriter>(inputType_, std::move(fileSink), std::make_shared<text::WriterOptions>());
    } else {
        VELOX_NYI("Only support for local filesystem and csv data format, others not supported.");
    }
}

void FileSystemDataSink::appendData(RowVectorPtr input) {
    writer_->write(input);
    writer_->flush();  
}

bool FileSystemDataSink::finish() {
    return writer_->finish();
}

void FileSystemDataSink::abort() {
    writer_->abort();
}

connector::DataSink::Stats FileSystemDataSink::stats() const {
    connector::DataSink::Stats stats;
    return stats;
}

std::vector<std::string> FileSystemDataSink::close() {
    std::vector<std::string> res;
    writer_->close();
    return res;
}

}