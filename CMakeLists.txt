cmake_minimum_required(VERSION 4.2)
project(sphinx VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list (APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND NOT CMAKE_CXX_COMPILER MATCHES "/usr/bin")
  add_link_options("-fuse-ld=lld" "-L/opt/homebrew/opt/llvm/lib/c++" "-Wl,-rpath,/opt/homebrew/opt/llvm/lib/c++")
endif()

option(ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)
if(ENABLE_SANITIZERS)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
if(ENABLE_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

enable_testing()
find_package(Arrow REQUIRED)


add_subdirectory(lib/logical-plan)
add_subdirectory(lib/types)
add_subdirectory(test)


add_executable(sphinx src/main.cpp)
target_link_libraries(sphinx PRIVATE logical_plan types)
