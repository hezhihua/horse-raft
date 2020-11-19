
add_custom_target(thirdparty)
include(ExternalProject)
 
 #gflags 库
set(GFLAG_ROOT          ${CMAKE_BINARY_DIR}/thirdparty/gflags-2.2.2)
set(GFLAG_LIB_DIR       ${GFLAG_ROOT}/lib)
set(GFLAG_INCLUDE_DIR   ${GFLAG_ROOT}/include)
 
#set(GFLAG_URL           https://github.com/gflags/gflags/archive/v2.2.2.tar.gz)
set(GFLAG_URL           https://github.com.cnpmjs.org/gflags/gflags/archive/v2.2.2.tar.gz)
set(GFLAG_CONFIGURE     cd ${GFLAG_ROOT}/src/gflags-2.2.2 && cmake  .)
set(GFLAG_MAKE          cd ${GFLAG_ROOT}/src/gflags-2.2.2 && make)
set(GFLAG_INSTALL       cd ${GFLAG_ROOT}/src/gflags-2.2.2 && make install)

ExternalProject_Add(gflags-2.2.2
        URL                   ${GFLAG_URL}
        DOWNLOAD_NAME         gflag-2.2.2.tar.gz
        PREFIX                ${GFLAG_ROOT}
        CONFIGURE_COMMAND     ${GFLAG_CONFIGURE}
        BUILD_COMMAND         ${GFLAG_MAKE}
        INSTALL_COMMAND       ${GFLAG_INSTALL}
)

add_dependencies(thirdparty gflags-2.2.2)


 #rocksdb 库
set(ROCKSDB_ROOT          ${CMAKE_BINARY_DIR}/thirdparty/rocksdb)
set(ROCKSDB_LIB_DIR       ${ROCKSDB_ROOT}/lib)
set(ROCKSDB_INCLUDE_DIR   ${ROCKSDB_ROOT}/include)
 
#set(ROCKSDB_URL           https://github.com/facebook/rocksdb/archive/v6.14.5.tar.gz)
set(ROCKSDB_URL           https://github.com.cnpmjs.org/facebook/rocksdb/archive/v6.14.5.tar.gz)
#set(ROCKSDB_CONFIGURE     cd ${ROCKSDB_ROOT}/src/rocksdb-6.14.5 && cmake -D CMAKE_INSTALL_PREFIX=${CMAKE_SOURCE_DIR}/third-party/rocksdb -DWITH_SNAPPY=ON .)
set(ROCKSDB_CONFIGURE     cd ${ROCKSDB_ROOT}/src/rocksdb-6.14.5 )
set(ROCKSDB_MAKE          cd ${ROCKSDB_ROOT}/src/rocksdb-6.14.5 && make static_lib)
set(ROCKSDB_INSTALL       cd ${ROCKSDB_ROOT}/src/rocksdb-6.14.5 && make PREFIX=${CMAKE_SOURCE_DIR}/third-party/rocksdb  install)

ExternalProject_Add(rocksdb-6.14.5
        URL                   ${ROCKSDB_URL}
        DOWNLOAD_NAME         rocksdb-6.14.5.tar.gz
        PREFIX                ${ROCKSDB_ROOT}
        CONFIGURE_COMMAND     ${ROCKSDB_CONFIGURE}
        BUILD_COMMAND         ${ROCKSDB_MAKE}
        INSTALL_COMMAND       ${ROCKSDB_INSTALL}
        BUILD_ALWAYS          1
)

add_dependencies(thirdparty rocksdb-6.14.5)

 #spdlog 库
set(SPDLOG_ROOT          ${CMAKE_BINARY_DIR}/thirdparty/spdlog)
 
#set(SPDLOG_URL           https://github.com/gabime/spdlog.git)
set(SPDLOG_URL           https://github.com.cnpmjs.org/gabime/spdlog.git)
set(SPDLOG_CONFIGURE     cd ${SPDLOG_ROOT}/src/spdlog-1.8.1 && cmake -D CMAKE_INSTALL_PREFIX=${CMAKE_SOURCE_DIR}/third-party/spdlog .)
set(SPDLOG_MAKE          cd ${SPDLOG_ROOT}/src/spdlog-1.8.1 && make)
set(SPDLOG_INSTALL       cd ${SPDLOG_ROOT}/src/spdlog-1.8.1 && make install)

ExternalProject_Add(spdlog-1.8.1
        GIT_REPOSITORY        ${SPDLOG_URL}
        GIT_TAG               v1.8.1
        PREFIX                ${SPDLOG_ROOT}
        CONFIGURE_COMMAND     ${SPDLOG_CONFIGURE}
        BUILD_COMMAND         ${SPDLOG_MAKE}
        INSTALL_COMMAND       ${SPDLOG_INSTALL}
        BUILD_ALWAYS          0
)
add_dependencies(thirdparty spdlog-1.8.1)





 #horse-rpc 库
set(RPC_ROOT          ${CMAKE_BINARY_DIR}/thirdparty/horse-rpc)
 
#set(RPC_URL           https://github.com/hezhihua/horse-rpc/archive/1.0.1.tar.gz)
set(RPC_URL           https://github.com.cnpmjs.org/hezhihua/horse-rpc.git)
#set(RPC_CONFIGURE     cd ${RPC_ROOT}/src/horse-rpc && cmake -D CMAKE_INSTALL_PREFIX=${CMAKE_SOURCE_DIR}/third-party/horse-rpc .)
set(RPC_CONFIGURE     cd ${RPC_ROOT}/src/horse-rpc && cmake -D CMAKE_INSTALL_PREFIX=${CMAKE_SOURCE_DIR}/third-party/horse-rpc .)
set(RPC_MAKE          cd ${RPC_ROOT}/src/horse-rpc && make )
set(RPC_INSTALL       cd ${RPC_ROOT}/src/horse-rpc && make  install)

ExternalProject_Add(horse-rpc
        GIT_REPOSITORY        ${RPC_URL}
        GIT_TAG               main
        PREFIX                ${RPC_ROOT}
        CONFIGURE_COMMAND     ${RPC_CONFIGURE}
        BUILD_COMMAND         ${RPC_MAKE}
        INSTALL_COMMAND       ${RPC_INSTALL}
        BUILD_ALWAYS          1
)


add_dependencies(horse-rpc spdlog-1.8.1)
add_dependencies(thirdparty horse-rpc)




 #yaml-cpp 库
set(YAMLCPP_ROOT          ${CMAKE_BINARY_DIR}/thirdparty/yaml-cpp)
set(YAMLCPP_LIB_DIR       ${YAMLCPP_ROOT}/lib)
set(YAMLCPP_INCLUDE_DIR   ${YAMLCPP_ROOT}/include)
 
#set(YAMLCPP_URL           https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.3.tar.gz)
set(YAMLCPP_URL           https://github.com.cnpmjs.org/jbeder/yaml-cpp/archive/yaml-cpp-0.6.3.tar.gz)
set(YAMLCPP_CONFIGURE     cd ${YAMLCPP_ROOT}/src/yaml-cpp && cmake -D CMAKE_INSTALL_PREFIX=${CMAKE_SOURCE_DIR}/third-party/yaml-cpp .)
set(YAMLCPP_MAKE          cd ${YAMLCPP_ROOT}/src/yaml-cpp && make)
set(YAMLCPP_INSTALL       cd ${YAMLCPP_ROOT}/src/yaml-cpp && make install)

ExternalProject_Add(yaml-cpp
        URL                   ${YAMLCPP_URL}
        DOWNLOAD_NAME         yaml-cpp-0.6.3.tar.gz
        PREFIX                ${YAMLCPP_ROOT}
        CONFIGURE_COMMAND     ${YAMLCPP_CONFIGURE}
        BUILD_COMMAND         ${YAMLCPP_MAKE}
        INSTALL_COMMAND       ${YAMLCPP_INSTALL}
        BUILD_ALWAYS          0
)


add_dependencies(thirdparty yaml-cpp)