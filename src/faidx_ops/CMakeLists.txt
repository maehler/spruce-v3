cmake_minimum_required(VERSION 3.0)

project(spruce-bins)

set (CMAKE_CXX_STANDARD 11)

include_directories(.)

if ($ENV{HOSTNAME} MATCHES "rackham[1-4].uppmax.uu.se")
  include_directories(/crex/proj/uppstore2017145/V3/software/include/fastxio)
  link_directories(/crex/proj/uppstore2017145/V3/software/lib)
endif()

add_library(spruceindex index.cpp MurmurHash3.cpp)

add_executable(build_index build_index.cpp)
add_executable(print_index print_index.cpp)
add_executable(sort_index sort_index.cpp)

find_package(Boost 1.56.0 REQUIRED COMPONENTS serialization)

if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
  target_link_libraries(build_index ${Boost_LIBRARIES})
  target_link_libraries(print_index ${Boost_LIBRARIES})
  target_link_libraries(sort_index ${Boost_LIBRARIES})
endif()

target_link_libraries(build_index spruceindex fastxio)
target_link_libraries(print_index spruceindex fastxio)
target_link_libraries(sort_index spruceindex fastxio)
