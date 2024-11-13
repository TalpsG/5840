# lab3a  选举实现
## ticker
这里ticker使用timer来实现
- electiontimer: candidate 和 follower 超时进入选举(term+1) ,leader不可能会出现electiontimer超时
- heartbeattimer: 只有leader可能出现heartbeattimer超时 广播心跳

## election
