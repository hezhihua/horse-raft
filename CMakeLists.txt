cmake_minimum_required(VERSION 2.8)
set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++1z -Wno-deprecated -Wall -pthread" )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-builtin-macro-redefined -D__FILE__='\"$(notdir $(abspath $<))\"'")
message(STATUS "optional:-g -std=c++1z -Wno-deprecated -Wall -pthread") 


include(cmake/thirdparty.cmake)

   SET(RAFT_SRCS  
      src/kv/DBBase.cpp
	  src/raft/RaftDB.cpp
	  src/raft/Storage.cpp
	  src/raft/RocksDBStorage.cpp
	  src/raft/Ballot.cpp
	  src/raft/BallotBox.cpp
	  src/raft/LogManager.cpp
	  src/raft/Configuration.cpp
	  src/raft/Lease.cpp
	  src/raft/Replicator.cpp
	  src/raft/FsmCaller.cpp
	  src/raft/AsyncLogThread.cpp
	  src/raft/NodeManager.cpp
	  src/raft/Node.cpp

      )   


INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/third-party/horse-rpc/include) 
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/spdlog/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/src/include) 
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/rocksdb/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/yaml-cpp/include)


LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/third-party/horse-rpc/lib)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/third-party/rocksdb/lib)
LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/third-party/yaml-cpp/lib)

link_libraries(horse-rpc)
link_libraries(yaml-cpp)
link_libraries(rocksdb)
link_libraries(snappy)


#ADD_LIBRARY(horse-raft SHARED ${RAFT_SRCS} )
ADD_LIBRARY(horse-raft STATIC ${RAFT_SRCS})
# 指定静态库的输出名称
set_target_properties(horse-raft PROPERTIES OUTPUT_NAME "horse-raft")

set_target_properties(horse-raft PROPERTIES CLEAN_DIRECT_OUTPUT 1)




SET (LIBRARY_OUTPUT_PATH  ${CMAKE_CURRENT_SOURCE_DIR}/lib)
# 安装horse-raft库和头文件
install(
	    TARGETS horse-raft
        ARCHIVE DESTINATION lib
        )
install(DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}/src/include DESTINATION ./ )