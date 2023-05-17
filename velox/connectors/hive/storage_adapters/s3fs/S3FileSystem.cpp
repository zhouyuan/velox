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

#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3RWFile.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/core/Context.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

namespace facebook::velox {

namespace filesystems {

class S3Config {
 public:
  S3Config(const Config* config) : config_(config) {}

  // Virtual addressing is used for AWS S3 and is the default (path-style-access
  // is false). Path access style is used for some on-prem systems like Minio.
  bool useVirtualAddressing() const {
    return !config_->get("hive.s3.path-style-access", false);
  }

  bool useSSL() const {
    return config_->get("hive.s3.ssl.enabled", true);
  }

  bool useInstanceCredentials() const {
    return config_->get("hive.s3.use-instance-credentials", false);
  }

  std::string endpoint() const {
    return config_->get("hive.s3.endpoint", std::string(""));
  }

  std::optional<std::string> accessKey() const {
    if (config_->isValueExists("hive.s3.aws-access-key")) {
      return config_->get("hive.s3.aws-access-key").value();
    }
    return {};
  }

  std::optional<std::string> secretKey() const {
    if (config_->isValueExists("hive.s3.aws-secret-key")) {
      return config_->get("hive.s3.aws-secret-key").value();
    }
    return {};
  }

  std::optional<std::string> iamRole() const {
    if (config_->isValueExists("hive.s3.iam-role")) {
      return config_->get("hive.s3.iam-role").value();
    }
    return {};
  }

  std::string iamRoleSessionName() const {
    return config_->get(
        "hive.s3.iam-role-session-name", std::string("velox-session"));
  }

  Aws::Utils::Logging::LogLevel getLogLevel() const {
    auto level = config_->get("hive.s3.log-level", std::string("FATAL"));
    // Convert to upper case.
    std::transform(
        level.begin(), level.end(), level.begin(), [](unsigned char c) {
          return std::toupper(c);
        });
    if (level == "FATAL") {
      return Aws::Utils::Logging::LogLevel::Fatal;
    } else if (level == "TRACE") {
      return Aws::Utils::Logging::LogLevel::Trace;
    } else if (level == "OFF") {
      return Aws::Utils::Logging::LogLevel::Off;
    } else if (level == "ERROR") {
      return Aws::Utils::Logging::LogLevel::Error;
    } else if (level == "WARN") {
      return Aws::Utils::Logging::LogLevel::Warn;
    } else if (level == "INFO") {
      return Aws::Utils::Logging::LogLevel::Info;
    } else if (level == "DEBUG") {
      return Aws::Utils::Logging::LogLevel::Debug;
    }
    return Aws::Utils::Logging::LogLevel::Fatal;
  }

 private:
  const Config* FOLLY_NONNULL config_;
};

class S3FileSystem::Impl {
 public:
  Impl(const Config* config) : s3Config_(config) {
    const size_t origCount = initCounter_++;
    if (origCount == 0) {
      Aws::SDKOptions awsOptions;
      awsOptions.loggingOptions.logLevel = s3Config_.getLogLevel();
      // In some situations, curl triggers a SIGPIPE signal causing the entire
      // process to be terminated without any notification.
      // This behavior is seen via Prestissimo on AmazonLinux2 on AWS EC2.
      // Relevant documentation in AWS SDK C++
      // https://github.com/aws/aws-sdk-cpp/blob/276ee83080fcc521d41d456dbbe61d49392ddf77/src/aws-cpp-sdk-core/include/aws/core/Aws.h#L96
      // This option allows the AWS SDK C++ to catch the SIGPIPE signal and
      // log a message.
      awsOptions.httpOptions.installSigPipeHandler = true;
      Aws::InitAPI(awsOptions);
    }
  }

  ~Impl() {
    const size_t newCount = --initCounter_;
    if (newCount == 0) {
      Aws::SDKOptions awsOptions;
      awsOptions.loggingOptions.logLevel = s3Config_.getLogLevel();
      Aws::ShutdownAPI(awsOptions);
    }
  }

  // Configure and return an AWSCredentialsProvider with access key and secret
  // key.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getAccessKeySecretKeyCredentialsProvider(
      const std::string& accessKey,
      const std::string& secretKey) const {
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
        awsString(accessKey), awsString(secretKey));
  }

  // Return a default AWSCredentialsProvider.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getDefaultCredentialsProvider() const {
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
  }

  // Configure and return an AWSCredentialsProvider with S3 IAM Role.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
  getIAMRoleCredentialsProvider(
      const std::string& s3IAMRole,
      const std::string& sessionName) const {
    return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
        awsString(s3IAMRole), awsString(sessionName));
  }

  // Return an AWSCredentialsProvider based on the config.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> getCredentialsProvider()
      const {
    auto accessKey = s3Config_.accessKey();
    auto secretKey = s3Config_.secretKey();
    const auto iamRole = s3Config_.iamRole();

    int keyCount = accessKey.has_value() + secretKey.has_value();
    // keyCount=0 means both are not specified
    // keyCount=2 means both are specified
    // keyCount=1 means only one of them is specified and is an error
    VELOX_USER_CHECK(
        (keyCount != 1),
        "Invalid configuration: both access key and secret key must be specified");

    int configCount = (accessKey.has_value() && secretKey.has_value()) +
        iamRole.has_value() + s3Config_.useInstanceCredentials();
    VELOX_USER_CHECK(
        (configCount <= 1),
        "Invalid configuration: specify only one among 'access/secret keys', 'use instance credentials', 'IAM role'");

    if (accessKey.has_value() && secretKey.has_value()) {
      return getAccessKeySecretKeyCredentialsProvider(
          accessKey.value(), secretKey.value());
    }

    if (s3Config_.useInstanceCredentials()) {
      return getDefaultCredentialsProvider();
    }

    if (iamRole.has_value()) {
      return getIAMRoleCredentialsProvider(
          iamRole.value(), s3Config_.iamRoleSessionName());
    }

    return getDefaultCredentialsProvider();
  }

  // Use the input Config parameters and initialize the S3Client.
  void initializeClient() {
    Aws::Client::ClientConfiguration clientConfig;

    clientConfig.endpointOverride = s3Config_.endpoint();

    if (s3Config_.useSSL()) {
      clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      clientConfig.scheme = Aws::Http::Scheme::HTTP;
    }

    auto credentialsProvider = getCredentialsProvider();

    client_ = std::make_shared<Aws::S3::S3Client>(
        credentialsProvider,
        clientConfig,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        s3Config_.useVirtualAddressing());
  }

  // Make it clear that the S3FileSystem instance owns the S3Client.
  // Once the S3FileSystem is destroyed, the S3Client fails to work
  // due to the Aws::ShutdownAPI invocation in the destructor.
  Aws::S3::S3Client* s3Client() const {
    return client_.get();
  }

  std::string getLogLevelName() const {
    return GetLogLevelName(s3Config_.getLogLevel());
  }

 private:
  const S3Config s3Config_;
  std::shared_ptr<Aws::S3::S3Client> client_;
  static std::atomic<size_t> initCounter_;
};

std::atomic<size_t> S3FileSystem::Impl::initCounter_(0);
folly::once_flag S3FSInstantiationFlag;

S3FileSystem::S3FileSystem(std::shared_ptr<const Config> config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

void S3FileSystem::initializeClient() {
  impl_->initializeClient();
}

std::string S3FileSystem::getLogLevelName() const {
  return impl_->getLogLevelName();
}

std::unique_ptr<ReadFile> S3FileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& /*unused*/) {
  const std::string file = s3Path(path);
  auto s3file = std::make_unique<S3ReadFile>(file, impl_->s3Client());
  s3file->initialize();
  return s3file;
}

std::unique_ptr<WriteFile> S3FileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  const std::string file = s3Path(path);
  auto s3file = std::make_unique<S3WriteFile>(file, impl_->s3Client());
  s3file->initialize();
  return s3file;
}

std::string S3FileSystem::name() const {
  return "S3";
}

static std::function<std::shared_ptr<FileSystem>(
    std::shared_ptr<const Config>,
    std::string_view)>
    filesystemGenerator = [](std::shared_ptr<const Config> properties,
                             std::string_view filePath) {
      // Only one instance of S3FileSystem is supported for now.
      // TODO: Support multiple S3FileSystem instances using a cache
      // Initialize on first access and reuse after that.
      static std::shared_ptr<FileSystem> s3fs;
      folly::call_once(S3FSInstantiationFlag, [&properties]() {
        std::shared_ptr<S3FileSystem> fs;
        if (properties != nullptr) {
          fs = std::make_shared<S3FileSystem>(properties);
        } else {
          fs = std::make_shared<S3FileSystem>(
              std::make_shared<core::MemConfig>());
        }
        fs->initializeClient();
        s3fs = fs;
      });
      return s3fs;
    };

void registerS3FileSystem() {
  registerFileSystem(isS3File, filesystemGenerator);
}

} // namespace filesystems

} // namespace facebook::velox
