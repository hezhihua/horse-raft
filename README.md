# horse-raft
An  C++ implementation of RAFT consensus algorithm based on horse-rpc

horse-raft的节点之间采用腾讯开源的tars协议来进行rpc通讯，腾讯开源的[tarscpp](https://github.com/TarsCloud/TarsCpp)里有不少代码是tars框架内互调的代码， 

如果完全搬来做raft比较冗余，因此基于tarscpp基础上阉割了不少代码，做了个简单版的rpc代码：[horse-rpc](https://github.com/hezhihua/horse-rpc)，

horse-raft基于[horse-rpc](https://github.com/hezhihua/horse-rpc) 做节点之间的rpc调用则可。    

# 依赖环境
| 软件	 | 要求 |
| ----- | ----- |
| gcc版本 | 最好4.8或以上 |
| cmake版本 | 3.10及以上版本 |
| rocksdb版本 | 6.11.4及以上版本 |
| yaml-cpp版本 | 0.6.3及以上版本 |
| horse-rpc | 最新版本 |
# 特性
1, Leader election,pre-vote,vote  
2, Log replication and recovery  
3, Snapshot and log compaction 

目前install Snapshot还没有实现,follower追加日志暂时采用AppendEntries的方式补全日志
# 编译和安装

1,git clone https://github.com/hezhihua/horse-raft.git  
2,cd horse-raft && mkdir build && cd build && cmake ..  && make 


# 二期   
1,timeoutNow,installSnapshot and log compaction     
2,支持管理命令,http接口

# 感谢
Baidu开源的[braft](https://github.com/baidu/braft)

# 学习和交流
QQ群:1124085420  
# 开源不易，如果喜欢本项目，请给我点个赞！  

