cmake_minimum_required(VERSION 3.10)

project(Redo)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS False)

add_subdirectory(fmt)
add_subdirectory(llfio)

add_executable(redo redo.cc)
target_include_directories(redo PUBLIC
  include
  fmt/include
  llfio/include
  build/quickcpplib/include
  build/outcome/include)
target_link_libraries(redo PUBLIC fmt::fmt pthread dl)
