cmake_minimum_required(VERSION 3.16)
project(pipeline VERSION 1.0.0 LANGUAGES CXX
        DESCRIPTION "Header-only C++17 library for pipelined, multi-threaded processing chains")

# Build examples/tests only when this is the top-level project.
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
  set(PIPELINE_IS_TOP_LEVEL ON)
else()
  set(PIPELINE_IS_TOP_LEVEL OFF)
endif()

option(PIPELINE_BUILD_EXAMPLES "Build the pipeline examples" ${PIPELINE_IS_TOP_LEVEL})
option(PIPELINE_BUILD_TESTS "Build the pipeline tests" ${PIPELINE_IS_TOP_LEVEL})
option(PIPELINE_INSTALL "Generate the install target" ${PIPELINE_IS_TOP_LEVEL})

include(GNUInstallDirs)
find_package(Threads REQUIRED)

add_library(pipeline INTERFACE)
add_library(pipeline::pipeline ALIAS pipeline)
target_compile_features(pipeline INTERFACE cxx_std_17)
target_link_libraries(pipeline INTERFACE Threads::Threads)
target_include_directories(pipeline INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

if(PIPELINE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(PIPELINE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(PIPELINE_INSTALL)
  include(CMakePackageConfigHelpers)
  install(DIRECTORY include/pipeline DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  install(TARGETS pipeline EXPORT pipelineTargets)
  install(EXPORT pipelineTargets
          NAMESPACE pipeline::
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/pipeline)
  configure_package_config_file(cmake/pipelineConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/pipelineConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/pipeline)
  write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/pipelineConfigVersion.cmake
    COMPATIBILITY SameMajorVersion ARCH_INDEPENDENT)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/pipelineConfig.cmake
                ${CMAKE_CURRENT_BINARY_DIR}/pipelineConfigVersion.cmake
          DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/pipeline)
endif()
