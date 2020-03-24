cmake_minimum_required(VERSION 3.10)

project(Redo)

add_subdirectory(fmt)

add_executable(redo redo.cc)
target_include_directories(redo PUBLIC fmt/include)
target_link_libraries(redo PUBLIC fmt::fmt)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
