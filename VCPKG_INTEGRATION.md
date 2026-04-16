# vcpkg Integration for Velox

This document describes the vcpkg-based dependency management system for Velox, which provides an alternative to the traditional script-based installation method.

## Overview

vcpkg is a cross-platform C/C++ package manager from Microsoft that simplifies dependency management. This integration allows Velox to use vcpkg for managing all its dependencies, providing better reproducibility and cross-platform support.

## Motivation

The traditional Velox build process relies on platform-specific scripts (e.g., `scripts/setup-macos.sh`) that:
- Download and build dependencies from source
- Require manual version management
- Have platform-specific implementations
- Can be time-consuming for initial builds

The vcpkg integration addresses these issues by:
- Providing a unified dependency management approach across platforms
- Enabling binary caching for faster builds
- Ensuring reproducible builds with locked dependency versions
- Simplifying maintenance and updates

## Quick Start

### Prerequisites

- CMake 3.28 or later
- Git
- C++20 compatible compiler
- Platform-specific build tools (make, ninja, etc.)

### Building with vcpkg

1. **Install and bootstrap vcpkg:**
   ```bash
   ./scripts/setup-vcpkg.sh install
   ```

2. **Configure the build:**
   ```bash
   ./scripts/setup-vcpkg.sh configure
   ```

3. **Build Velox:**
   ```bash
   cmake --build _build -j$(nproc)
   ```

### Enabling Optional Features

To enable optional features like S3, GCS, or ABFS support:

```bash
export VCPKG_FEATURE_FLAGS="s3;gcs;abfs"
./scripts/setup-vcpkg.sh configure
cmake --build _build -j$(nproc)
```

## Architecture

### Directory Structure

```
velox/
├── dev/vcpkg/
│   ├── vcpkg.json                    # Dependency manifest
│   ├── vcpkg-configuration.json      # vcpkg configuration
│   ├── triplets/                     # Custom build configurations
│   │   ├── arm64-osx-release.cmake
│   │   ├── x64-osx-release.cmake
│   │   ├── arm64-linux-release.cmake
│   │   └── x64-linux-release.cmake
│   ├── ports/                        # Custom package overlays
│   └── README.md
├── scripts/
│   └── setup-vcpkg.sh                # vcpkg setup script
└── CMakeLists.txt                    # Updated with vcpkg support
```

### Key Components

#### 1. vcpkg.json (Manifest File)

Declares all Velox dependencies with specific version constraints:

```json
{
  "name": "velox",
  "dependencies": [
    "folly",
    "glog",
    "gflags",
    ...
  ],
  "features": {
    "s3": { "dependencies": ["aws-sdk-cpp"] },
    "gcs": { "dependencies": ["google-cloud-cpp"] }
  },
  "overrides": [
    { "name": "fmt", "version": "11.2.0" }
  ]
}
```

#### 2. vcpkg-configuration.json

Configures vcpkg registries and overlays:

```json
{
  "overlay-ports": ["./ports"],
  "overlay-triplets": ["./triplets"],
  "registries": [...]
}
```

#### 3. Custom Triplets

Define platform-specific build configurations. All triplets:
- Build dependencies as static libraries (except glog/gflags)
- Use C++20 standard
- Target release builds by default

#### 4. CMake Integration

The main `CMakeLists.txt` detects vcpkg usage and automatically sets:
- `VELOX_DEPENDENCY_SOURCE=VCPKG`
- `VCPKG_MANIFEST_DIR` to `dev/vcpkg`

The `ResolveDependency.cmake` module handles vcpkg dependencies through `find_package()`.

## Dependency Versions

The following dependency versions are pinned in the manifest:

| Package | Version | Notes |
|---------|---------|-------|
| fmt | 11.2.0 | Matches Velox requirement |
| protobuf | 21.8 | Compatible with Velox |
| xsimd | 10.0.0 | SIMD library |
| glog | 0.6.0 | Logging library (shared) |
| gflags | 2.2.2 | Command-line flags (shared) |
| re2 | 2024-07-02 | Regular expressions |
| abseil | 20240116.2 | Google utilities |
| grpc | 1.48.1 | RPC framework |
| google-cloud-cpp | 2.22.0 | GCS support |
| simdjson | 4.1.0 | JSON parsing |
| thrift | 0.16.0 | Serialization |

See `dev/vcpkg/vcpkg.json` for the complete list.

## Features

The vcpkg manifest supports the following optional features:

### Storage Adapters

- **s3**: AWS S3 support via aws-sdk-cpp
- **gcs**: Google Cloud Storage via google-cloud-cpp
- **abfs**: Azure Blob Storage via azure-storage-*-cpp
- **hdfs**: Hadoop Distributed File System support

### Additional Features

- **duckdb**: DuckDB integration
- **arrow**: Apache Arrow support

### Enabling Features

Features can be enabled in multiple ways:

1. **Via environment variable:**
   ```bash
   export VCPKG_FEATURE_FLAGS="s3;gcs"
   ./scripts/setup-vcpkg.sh configure
   ```

2. **Via CMake:**
   ```bash
   cmake -DVCPKG_MANIFEST_FEATURES="s3;gcs" ...
   ```

3. **In vcpkg.json:**
   ```json
   {
     "default-features": ["s3", "gcs"]
   }
   ```

## Platform Support

### macOS

Supported architectures:
- **arm64** (Apple Silicon): Use `arm64-osx-release` triplet
- **x86_64** (Intel): Use `x64-osx-release` triplet

The setup script auto-detects the architecture.

### Linux

Supported architectures:
- **x86_64**: Use `x64-linux-release` triplet
- **aarch64** (ARM64): Use `arm64-linux-release` triplet

### Windows

Windows support can be added by creating appropriate triplet files (e.g., `x64-windows-release.cmake`).

## Comparison with Traditional Build

### Traditional Method

```bash
# Install dependencies via scripts
./scripts/setup-macos.sh

# Build
make
```

**Pros:**
- Familiar to existing developers
- Direct control over build process

**Cons:**
- Platform-specific scripts
- Long initial build times
- Manual version management
- Difficult to reproduce builds

### vcpkg Method

```bash
# Install vcpkg and configure
./scripts/setup-vcpkg.sh install
./scripts/setup-vcpkg.sh configure

# Build
cmake --build _build -j$(nproc)
```

**Pros:**
- Cross-platform consistency
- Binary caching (faster rebuilds)
- Reproducible builds
- Easier dependency updates
- Better CI/CD integration

**Cons:**
- Additional vcpkg installation step
- Learning curve for vcpkg concepts

## Advanced Usage

### Using a Specific vcpkg Version

```bash
export VCPKG_ROOT=/path/to/vcpkg
cd $VCPKG_ROOT
git checkout <commit-hash>
./bootstrap-vcpkg.sh
cd /path/to/velox
./scripts/setup-vcpkg.sh configure
```

### Binary Caching

Enable vcpkg binary caching to speed up builds:

```bash
export VCPKG_BINARY_SOURCES="clear;files,/path/to/cache,readwrite"
./scripts/setup-vcpkg.sh configure
```

### Custom Port Overlays

To patch a dependency, create a port overlay in `dev/vcpkg/ports/`:

```bash
mkdir -p dev/vcpkg/ports/mypackage
# Copy and modify the port files
```

The overlay will be automatically used due to the configuration in `vcpkg-configuration.json`.

### Debug Builds

To build dependencies in debug mode, create a debug triplet:

```cmake
# dev/vcpkg/triplets/arm64-osx-debug.cmake
set(VCPKG_BUILD_TYPE debug)
# ... other settings
```

Then use it:

```bash
export VCPKG_TRIPLET=arm64-osx-debug
./scripts/setup-vcpkg.sh configure
```

## Troubleshooting

### Clean Build

If you encounter issues, try a clean build:

```bash
./scripts/setup-vcpkg.sh clean
rm -rf _build
./scripts/setup-vcpkg.sh install
./scripts/setup-vcpkg.sh configure
```

### Dependency Not Found

Ensure the dependency is listed in `dev/vcpkg/vcpkg.json` and that vcpkg is properly bootstrapped:

```bash
cd vcpkg
./vcpkg list
```

### Version Conflicts

Check the `overrides` section in `vcpkg.json` to ensure version compatibility.

### Build Failures

1. Check vcpkg logs: `_build/vcpkg_installed/vcpkg/buildtrees/`
2. Verify triplet settings match your platform
3. Ensure all required system dependencies are installed

## Migration Guide

### For Existing Developers

If you're currently using the script-based build:

1. **Keep your existing setup** - Both methods can coexist
2. **Try vcpkg in a new directory:**
   ```bash
   git clone <velox-repo> velox-vcpkg
   cd velox-vcpkg
   ./scripts/setup-vcpkg.sh install
   ./scripts/setup-vcpkg.sh configure
   cmake --build _build -j$(nproc)
   ```
3. **Compare build times and experience**
4. **Switch when comfortable**

### For CI/CD

Update your CI configuration to use vcpkg:

```yaml
# Example GitHub Actions
- name: Setup vcpkg
  run: ./scripts/setup-vcpkg.sh install

- name: Configure
  run: ./scripts/setup-vcpkg.sh configure

- name: Build
  run: cmake --build _build -j$(nproc)
```

## Contributing

When adding new dependencies:

1. Add to `dev/vcpkg/vcpkg.json` dependencies list
2. If a specific version is needed, add to `overrides`
3. If the package needs patches, create a port overlay in `dev/vcpkg/ports/`
4. Update this documentation
5. Test on all supported platforms

## References

- [vcpkg Documentation](https://vcpkg.io/)
- [vcpkg Manifest Mode](https://vcpkg.io/en/docs/users/manifests.html)
- [vcpkg Triplets](https://vcpkg.io/en/docs/users/triplets.html)
- [Gluten vcpkg Integration PR](https://github.com/apache/gluten/pull/11563)
- [Velox Build Documentation](docs/develop/build.rst)

## Support

For issues or questions:
- File an issue on the Velox GitHub repository
- Tag with `vcpkg` label
- Include platform, vcpkg version, and error logs