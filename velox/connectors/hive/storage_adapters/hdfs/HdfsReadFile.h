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

#include "velox/common/file/File.h"
#ifdef VELOX_ENABLE_HDFS
#include "velox/external/hdfs/HdfsInternal.h"
#endif

#ifdef VELOX_ENABLE_HDFS3
#include <hdfs/hdfs.h>
#endif

namespace facebook::velox {

#ifdef VELOX_ENABLE_HDFS
namespace filesystems::arrow::io::internal {
class LibHdfsShim;
}
#endif

/**
 * Implementation of hdfs read file.
 */
class HdfsReadFile final : public ReadFile {
 public:
#ifdef VELOX_ENABLE_HDFS
  explicit HdfsReadFile(
      filesystems::arrow::io::internal::LibHdfsShim* driver,
      hdfsFS hdfs,
      std::string_view path);
#endif

#ifdef VELOX_ENABLE_HDFS3
  explicit HdfsReadFile(hdfsFS hdfs, std::string_view path);
#endif
  ~HdfsReadFile() override;

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final {
    return filePath_;
  }

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;
  void checkFileReadParameters(uint64_t offset, uint64_t length) const;

#ifdef VELOX_ENABLE_HDFS
  filesystems::arrow::io::internal::LibHdfsShim* driver_;
#endif
  hdfsFS hdfsClient_;
  hdfsFileInfo* fileInfo_;
  std::string filePath_;
};

} // namespace facebook::velox
