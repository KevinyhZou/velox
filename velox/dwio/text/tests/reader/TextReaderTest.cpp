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
#include "velox/vector/tests/utils/VectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/text/writer/TextWriter.h"

namespace facebook::velox::test {
class TextReaderTest : public testing::Test,
                       public velox::test::VectorTestBase {
public:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    rootPool_ = memory::memoryManager()->addRootPool("TextReaderTests");
    leafPool_ = rootPool_->addLeafChild("TextReaderTests");
    tempPath_ = exec::test::TempDirectoryPath::create();
    initFileData();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void initFileData() {
    auto schema = ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
          {BOOLEAN(),
           TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           TIMESTAMP(),
           VARCHAR()});
    auto data = makeRowVector({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
      {
          makeConstant(true, 3),
          makeFlatVector<int8_t>({1, 2, 3}),
          makeFlatVector<int16_t>({1, 2, 3}), // TODO null
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<float>({1.1, kInf, 3.1}),
          makeFlatVector<double>({1.1, kNaN, 3.1}),
          makeFlatVector<Timestamp>(
              3, [](auto i) { return Timestamp(i, i * 1'000'000); }),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARCHAR())});

    text::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    auto filePath =
      fs::path(fmt::format("{}/test_text_reader.txt", tempPath_->getPath()));
    auto sink = std::make_unique<dwio::common::LocalFileSink>(
      filePath, dwio::common::FileSink::Options{.pool = leafPool_.get()});
    auto writer = std::make_unique<text::TextWriter>(
      schema,
      std::move(sink),
      std::make_shared<text::WriterOptions>(writerOptions));
    writer->write(data);
    writer->close();
  }

  constexpr static float kInf = std::numeric_limits<float>::infinity();
  constexpr static double kNaN = std::numeric_limits<double>::quiet_NaN();
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;

};


}