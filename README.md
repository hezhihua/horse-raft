# horse-raft
An  C++ implementation of RAFT consensus algorithm based on horse-rpc

# 依赖环境
| 软件	 | 要求 |
| ----- | ----- |
| gcc版本 | 最好4.8或以上 |
| cmake版本 | 3.2及以上版本 |
| rocksdb版本 | 6.11.4及以上版本 |
| yaml-cpp-yaml-cpp版本 | 0.6.3及以上版本 |
| horse-rpc | 最新版本 |
# 特性
1, Leader election,pre-vote,vote  
2, Log replication and recovery  
3, Snapshot and log compaction --todo


# 编译和安装

1,git clone https://github.com/hezhihua/horse-raft.git  
2,mkdir build && cd build && cmake ..  && make 


# TODO   
1,Snapshot and log compaction     


# 感谢
Baidu开源的braft

# 学习和交流
QQ群:1124085420  
