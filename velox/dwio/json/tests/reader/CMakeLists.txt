# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(velox_dwio_json_reader_test JsonReaderTest.cpp)

add_test(
  NAME velox_dwio_json_reader_test
  COMMAND velox_dwio_json_reader_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_dwio_json_reader_test
  velox_dwio_json_reader
  velox_dwio_json_reader_register
  velox_dwio_common
  velox_dwio_common_exception
  velox_memory
  velox_type
  velox_vector
  velox_vector_test_lib
  gtest
  gtest_main
  gmock
  Folly::folly)