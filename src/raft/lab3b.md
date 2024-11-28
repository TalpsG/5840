# lab3b
## 方案
- 周期性的发送heartbeat当做appendentries，即放弃start中调用appendentries 来降低复杂度
- 每次hb不保证rpc一定成功，如果rpc成功，则根据log duplication的情况分别处理
  - 如果match 和 lastlog 不相同，则一次性发送match - lastlog 的所有日志
  - 心跳当做appendentries，这样的话就不需要管理rpc失败的处理了

## 修改
1. appendentries args
2. 心跳成功时处理 match_idx next_idx 和commit_idx
3. appendentries
4. voterequest


## log
11.24 
log复制的时候出现下标错误


## tips
1. follower -> leader的时候需要初始化follower_next_idx 和 follower_match_idx

