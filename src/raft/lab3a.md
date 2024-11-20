# lab3a  选举实现
## ticker
这里ticker使用timer来实现
- electiontimer: candidate 和 follower 超时进入选举(term+1) ,leader不可能会出现electiontimer超时
- heartbeattimer: 只有leader可能出现heartbeattimer超时 广播心跳

## election


## 问题
1. 如果在转换状态时关闭所有的timer再挑选需要的reset，那么如果此时timer在stop之前已经timeout了，那么会发生什么？
  - ticker进入electiontimer 然后 投票完成 转换为leader，发送hb。此时如果发送完hb后进入ticker，则ticker内会把状态转换回去到Candidate.
  - 如果先进入ticker 后发送hb，则hb发送的都无效了(因为term++了)
