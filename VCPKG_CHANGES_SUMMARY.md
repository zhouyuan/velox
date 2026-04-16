# vcpkg Integration - Changes Summary

This document summarizes all changes made to integrate vcpkg-based dependency management into Velox, ported from the Gluten project.

## Files Created

### 1. vcpkg Configuration Files

#### `dev/vcpkg/vcpkg.json`
- **Purpose**: Main vcpkg manifest file declaring all Velox dependencies
- **Key Features**:
  - Lists all core dependencies (folly, glog, gflags, boost, protobuf, etc.)
  - Defines optional features (s3, gcs, abfs, hdfs, duckdb, arrow)
  - Pins specific versions via overrides section
  - Uses vcpkg baseline: `4334d8b4c8916018600212ab4dd4bbdc343065d1`

#### `dev/vcpkg/vcpkg-configuration.json`
- **Purpose**: vcpkg configuration for registries and overlays
- **Key Features**:
  - Configures overlay-ports and overlay-triplets directories
  - Sets up custom registries for google-cloud-cpp and upb packages
  - Enables custom port overlays for package patches

### 2. Triplet Files (Platform-Specific Build Configurations)

#### `dev/vcpkg/triplets/arm64-osx-release.cmake`
- **Platform**: macOS ARM64 (Apple Silicon)
- **Configuration**: Static libraries, release build, C++20, shared glog/gflags

#### `dev/vcpkg/triplets/x64-osx-release.cmake`
- **Platform**: macOS x86_64 (Intel)
- **Configuration**: Static libraries, release build, C++20, shared glog/gflags

#### `dev/vcpkg/triplets/arm64-linux-release.cmake`
- **Platform**: Linux ARM64
- **Configuration**: Static libraries, release build, C++20, shared glog/gflags

#### `dev/vcpkg/triplets/x64-linux-release.cmake`
- **Platform**: Linux x86_64
- **Configuration**: Static libraries, release build, C++20, shared glog/gflags

### 3. Setup Script

#### `scripts/setup-vcpkg.sh`
- **Purpose**: Automated vcpkg installation and configuration script
- **Commands**:
  - `install`: Clone and bootstrap vcpkg
  - `configure`: Configure CMake with vcpkg toolchain
  - `clean`: Remove vcpkg installation
  - `help`: Show usage information
- **Features**:
  - Auto-detects platform and architecture
  - Supports environment variables for customization
  - Handles vcpkg feature flags

### 4. Documentation

#### `dev/vcpkg/README.md`
- **Purpose**: Quick reference guide for vcpkg integration
- **Contents**:
  - Quick start instructions
  - Feature descriptions
  - Triplet information
  - Troubleshooting tips
  - Comparison with traditional setup

#### `VCPKG_INTEGRATION.md`
- **Purpose**: Comprehensive documentation for vcpkg integration
- **Contents**:
  - Detailed architecture overview
  - Complete dependency version list
  - Advanced usage scenarios
  - Migration guide
  - Troubleshooting section
  - Contributing guidelines

#### `VCPKG_CHANGES_SUMMARY.md` (this file)
- **Purpose**: Summary of all changes made for vcpkg integration

## Files Modified

### 1. `CMakeLists.txt`
**Changes**:
- Added vcpkg toolchain detection (lines 36-47)
- Automatically sets `VELOX_DEPENDENCY_SOURCE=VCPKG` when vcpkg toolchain is detected
- Sets `VCPKG_MANIFEST_DIR` to `dev/vcpkg` if not already defined
- Maintains backward compatibility with existing build methods

**Location**: After project() declaration, before Conda environment handling

### 2. `CMake/ResolveDependency.cmake`
**Changes**:
- Added support for `VCPKG` dependency source in `velox_resolve_dependency` macro
- When `${dependency_name}_SOURCE` is `VCPKG`, uses `find_package()` with REQUIRED
- Maintains backward compatibility with AUTO, SYSTEM, and BUNDLED sources

**Location**: In the `velox_resolve_dependency` macro (lines 65-89)

## Directory Structure Created

```
velox/
├── dev/
│   └── vcpkg/
│       ├── vcpkg.json
│       ├── vcpkg-configuration.json
│       ├── README.md
│       ├── triplets/
│       │   ├── arm64-osx-release.cmake
│       │   ├── x64-osx-release.cmake
│       │   ├── arm64-linux-release.cmake
│       │   └── x64-linux-release.cmake
│       └── ports/                    (empty, for future overlays)
├── scripts/
│   └── setup-vcpkg.sh               (new)
├── VCPKG_INTEGRATION.md             (new)
└── VCPKG_CHANGES_SUMMARY.md         (new)
```

## Key Design Decisions

### 1. Dependency Versions
- Matched versions from `scripts/setup-versions.sh` where possible
- Used vcpkg overrides to pin specific versions
- Ensured compatibility with existing Velox requirements

### 2. Build Configuration
- Static linking for most dependencies (reduces runtime dependencies)
- Shared linking for glog and gflags (avoids dual flag registration issues)
- Release builds by default (optimized for production use)
- C++20 standard (matches Velox requirement)

### 3. Feature Organization
- Core dependencies in main dependencies list
- Optional features (s3, gcs, abfs, hdfs, duckdb, arrow) as vcpkg features
- Allows selective dependency installation

### 4. Platform Support
- Separate triplets for each platform/architecture combination
- Auto-detection in setup script
- Easy to extend for additional platforms

### 5. Backward Compatibility
- Existing build methods continue to work
- vcpkg is opt-in via toolchain file
- No breaking changes to existing workflows

## Usage Examples

### Basic Build
```bash
./scripts/setup-vcpkg.sh install
./scripts/setup-vcpkg.sh configure
cmake --build _build -j$(nproc)
```

### Build with S3 and GCS Support
```bash
export VCPKG_FEATURE_FLAGS="s3;gcs"
./scripts/setup-vcpkg.sh install
./scripts/setup-vcpkg.sh configure
cmake --build _build -j$(nproc)
```

### Manual CMake Configuration
```bash
cmake -B _build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=arm64-osx-release \
  -DVCPKG_MANIFEST_DIR=dev/vcpkg \
  -DVCPKG_MANIFEST_FEATURES="s3;gcs"
cmake --build _build -j$(nproc)
```

## Testing Recommendations

Before merging, test on:
1. **macOS ARM64** (Apple Silicon)
2. **macOS x86_64** (Intel)
3. **Linux x86_64**
4. **Linux ARM64** (if available)

Test scenarios:
1. Fresh build with vcpkg
2. Build with various feature combinations
3. Verify all dependencies are correctly linked
4. Run existing test suites
5. Compare build times with traditional method

## Benefits

1. **Cross-Platform Consistency**: Same build process across all platforms
2. **Reproducibility**: Locked dependency versions ensure consistent builds
3. **Binary Caching**: Faster rebuilds with vcpkg binary cache
4. **Easier Maintenance**: Centralized dependency management
5. **Better CI/CD**: Simplified CI configuration
6. **Flexibility**: Easy to enable/disable features

## Migration Path

For existing users:
1. Both build methods can coexist
2. Try vcpkg in a separate directory first
3. Gradually migrate CI/CD pipelines
4. Eventually deprecate script-based method (optional)

## Future Enhancements

Potential improvements:
1. Add Windows triplet files
2. Create custom port overlays for packages requiring patches
3. Set up vcpkg binary cache for CI
4. Add vcpkg registry for Velox-specific packages
5. Integrate with package managers (Conan, etc.)

## References

- **Source**: [Gluten PR #11563](https://github.com/apache/gluten/pull/11563)
- **vcpkg Documentation**: https://vcpkg.io/
- **Velox Repository**: https://github.com/facebookincubator/velox

## Credits

This integration is based on the vcpkg implementation in the Apache Gluten project, adapted for Velox's specific requirements and dependency versions.