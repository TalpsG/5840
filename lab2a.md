# leader election
## goal
1. new leader to be elected
2. nothing happened if there is a leader
3. new leader to take over if old leader crash or packets lost.

## hint 
1. 看图2的选举规则和状态
2. 看图2，自己添加状态信息和日志信息到raft.go文件中
3. 编写`RequestVoteArgs/Reply`结构体，修改`Make`来创建后台的goroutine在超时后周期性的发送`RequestVote` 




## tips
1. leader和candidate节点的票相当于投给了自己，因此leader收到term与自己相同的requestvote时是不会进行投票的
2. 心跳包和正常的appendentries是一样的，只不过携带的entries为0个，即心跳包也需要进行与其他节点的日志比较，以及日志恢复


## todo
1. election中的sendRequestvote 当收到的reply中term大于curr_term时自己要转变为follower
2. heartbeat 中的sendHeartBeat 同第一条

## 10.14 todo
1. 票数判断放在RequestVote的goroutine当中
2. ticker中leader timeout 的逻辑处理
3. 为了避免一个term中多次投票
  - follower: 
    - term newer: appendentries 或 requestvote  投票或记录心跳
    - term equal: 没投过票才会投票以及记录心跳
  - candidate: 
    - term newer: appendentries转为follower ,requestvote 投票
    - term equal: append 转换为follower 但 vote_for不变，requestvote不投票，因为已经投给自己了
  - leader:
    - term newer: appendentries转为follower ,requestvote 投票
    - term equal: appendentries 不可能出现，一个term只有一个leader。 requestvote无视

