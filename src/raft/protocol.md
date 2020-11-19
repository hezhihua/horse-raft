
key->value 说明:

Raft Log       :  0x02 + 组id(8字节大端序) + 0x01 + LogIndex（8字节大端序）  -> LogEntry
RaftLocalState :  0x02 + 组id(8字节大端序) + 0x02                           ->
RaftApplyState :  0x02 + 组id(8字节大端序) + 0x03                           ->
当前节点选举信息 :  0x02 + 组id(8字节大端序) + 0x04                           ->VoteInfoMeta
集群地址信息    :  0x02 + 组id(8字节大端序) + 0x05                           ->ConfigurationMeta


RegionLocalState:0x03 + 组id(8字节大端序) + 0x01