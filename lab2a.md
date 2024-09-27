# leader election
## goal
1. new leader to be elected
2. nothing happened if there is a leader
3. new leader to take over if old leader crash or packets lost.

## hint 
1. 看图2的选举规则和状态
2. 看图2，自己添加状态信息和日志信息到raft.go文件中
3. 编写`RequestVoteArgs/Reply`结构体，修改`Make`来创建后台的goroutine在超时后周期性的发送`RequestVote` 
