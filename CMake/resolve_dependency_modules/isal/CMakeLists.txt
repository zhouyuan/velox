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
project(ISAL)

if(VELOX_ENABLE_ISAL)
message(STATUS "Building isal from source")
set(ISAL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/isal_ep-install")
set(ISAL_INCLUDE_DIR "${ISAL_PREFIX}/include")
set(ISAL_STATIC_LIB "${ISAL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/libisal.a")

set(ISAL_CONFIGURE_ARGS
    "--prefix=/"
    "AR=${CMAKE_AR}"
    "RANLIB=${CMAKE_RANLIB}"
    "CC=${CMAKE_C_COMPILER}"
    "CXX=${CMAKE_CXX_COMPILER}"
    "CFLAGS=-fPIC -O3"
    "CXXFLAGS=-fPIC -O3")
set(ISAL_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
ExternalProject_Add(isal_ep
                    PREFIX isal_ep
                    UPDATE_COMMAND "./autogen.sh"
                    CONFIGURE_COMMAND "./configure" ${ISAL_CONFIGURE_ARGS}
                    BUILD_COMMAND ${ISAL_BUILD_COMMAND}
                    INSTALL_COMMAND ${MAKE} install DESTDIR=${ISAL_PREFIX}
                    BUILD_BYPRODUCTS ${ISAL_STATIC_LIB}
                    BUILD_IN_SOURCE 1
                    URL "https://github.com/intel/isa-l/archive/refs/tags/v2.30.0.tar.gz"
)
file(MAKE_DIRECTORY "${ISAL_INCLUDE_DIR}")
add_library(ISAL::isal STATIC IMPORTED GLOBAL)
set_target_properties(
  ISAL::isal
  PROPERTIES IMPORTED_LOCATION "${ISAL_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
             "${ISAL_INCLUDE_DIR}")
add_dependencies(ISAL::isal isal_ep)
include_directories(SYSTEM ${ISAL_INCLUDE_DIR})

endif()
