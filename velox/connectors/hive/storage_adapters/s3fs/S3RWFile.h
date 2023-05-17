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

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/dwio/common/DataSink.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace facebook::velox {
class S3FileSink : public facebook::velox::dwio::common::DataSink {
 public:
  explicit S3FileSink(
      const std::string& fullDestinationPath,
      const facebook::velox::dwio::common::MetricsLogPtr& metricLogger =
          facebook::velox::dwio::common::MetricsLog::voidLog(),
      facebook::velox::dwio::common::IoStatistics* stats = nullptr)
      : facebook::velox::dwio::common::DataSink{
            "S3FileSink",
            metricLogger,
            stats} {
    std::string destinationPath = fullDestinationPath.substr(6);
    auto s3FileSystem =
        filesystems::getFileSystem(fullDestinationPath, nullptr);
    file_ = s3FileSystem->openFileForWrite(destinationPath);
  }

  ~S3FileSink() override {
    destroy();
  }

  using facebook::velox::dwio::common::DataSink::write;

  void write(std::vector<facebook::velox::dwio::common::DataBuffer<char>>&
                 buffers) override {
    writeImpl(buffers, [&](auto& buffer) {
      size_t size = buffer.size();
      std::string str(buffer.data(), size);
      file_->append(str);
      return size;
    });
  }

  static void registerFactory();

 protected:
  void doClose() override {
    file_->close();
  }

 private:
  std::unique_ptr<WriteFile> file_;
};
namespace {
// Reference: https://issues.apache.org/jira/browse/ARROW-8692
// https://github.com/apache/arrow/blob/master/cpp/src/arrow/filesystem/s3fs.cc#L843
// A non-copying iostream. See
// https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf,
                         public std::iostream {
 public:
  StringViewStream(const void* data, int64_t nbytes)
      : Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
            static_cast<size_t>(nbytes)),
        std::iostream(this) {}
};

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read directly into a pre-allocated buffer instead.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes) {
  return [=]() { return Aws::New<StringViewStream>("", data, nbytes); };
}

class S3ReadFile final : public ReadFile {
 public:
  S3ReadFile(const std::string& path, Aws::S3::S3Client* client)
      : client_(client) {
    bucketAndKeyFromS3Path(path, bucket_, key_);
  }

  // Gets the length of the file.
  // Checks if there are any issues reading the file.
  void initialize() {
    // Make it a no-op if invoked twice.
    if (length_ != -1) {
      return;
    }

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));

    auto outcome = client_->HeadObject(request);
    VELOX_CHECK_AWS_OUTCOME(
        outcome, "Failed to get metadata for S3 object", bucket_, key_);
    length_ = outcome.GetResult().GetContentLength();
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    std::string result(length, 0);
    char* position = result.data();
    preadInternal(offset, length, position);
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    // 'buffers' contains Ranges(data, size)  with some gaps (data = nullptr) in
    // between. This call must populate the ranges (except gap ranges)
    // sequentially starting from 'offset'. AWS S3 GetObject does not support
    // multi-range. AWS S3 also charges by number of read requests and not size.
    // The idea here is to use a single read spanning all the ranges and then
    // populate individual ranges. We pre-allocate a buffer to support this.
    size_t length = 0;
    for (const auto range : buffers) {
      length += range.size();
    }
    // TODO: allocate from a memory pool
    std::string result(length, 0);
    preadInternal(offset, length, static_cast<char*>(result.data()));
    size_t resultOffset = 0;
    for (auto range : buffers) {
      if (range.data()) {
        memcpy(range.data(), &(result.data()[resultOffset]), range.size());
      }
      resultOffset += range.size();
    }
    return length;
  }

  uint64_t size() const override {
    return length_;
  }

  uint64_t memoryUsage() const override {
    // TODO: Check if any buffers are being used by the S3 library
    return sizeof(Aws::S3::S3Client) + kS3MaxKeySize + 2 * sizeof(std::string) +
        sizeof(int64_t);
  }

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const final {
    return fmt::format("s3://{}/{}", bucket_, key_);
  }

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

 private:
  // The assumption here is that "position" has space for at least "length"
  // bytes.
  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    // Read the desired range of bytes.
    Aws::S3::Model::GetObjectRequest request;
    Aws::S3::Model::GetObjectResult result;

    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));
    std::stringstream ss;
    ss << "bytes=" << offset << "-" << offset + length - 1;
    request.SetRange(awsString(ss.str()));
    request.SetResponseStreamFactory(
        AwsWriteableStreamFactory(position, length));
    auto outcome = client_->GetObject(request);
    VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to get S3 object", bucket_, key_);
  }

  Aws::S3::S3Client* client_;
  std::string bucket_;
  std::string key_;
  int64_t length_ = -1;
};

class S3WriteFile final : public WriteFile {
 public:
  S3WriteFile(const std::string& path, Aws::S3::S3Client* client)
      : client_(client) {
    bucketAndKeyFromS3Path(path, bucket_, key_);
  }

  void initialize() {}

  void append(std::string_view data) override {
    if (data.size() == 0) {
      return;
    }
    size_ += data.size();
    localStr_ << data;
  }

  void close() override {
    std::shared_ptr<Aws::IOStream> inputData =
        Aws::MakeShared<Aws::StringStream>("");
    *inputData << localStr_.str();
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(awsString(bucket_));
    request.SetKey(awsString(key_));
    request.SetBody(inputData);
    auto outcome = client_->PutObject(request);
    if (!outcome.IsSuccess()) {
      std::cerr << "Error: PutObjectBuffer: " << outcome.GetError().GetMessage()
                << std::endl;
    } else {
      std::cout << "Success: Object" << std::endl;
    }
    localStr_.clear();
  }

  void flush() override {}

  uint64_t size() const override {
    return size_;
  }

 private:
  Aws::S3::S3Client* client_;
  std::string bucket_;
  std::string key_;
  uint64_t size_;
  std::stringstream localStr_;
};
} // namespace
} // namespace facebook::velox
