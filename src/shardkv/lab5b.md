# lab5b
## shardkv 
lab5 的整个框架是这样的
groups代表一个raft集群，内有n个server做数据备份
shardkv 代表一个 shardkv 服务，一个kv服务内有许多个groups,每个groups会负责 一些分片

shardctrler 是保存整个shardkv的配置信息
query: 用来查询配置
join:添加groups到shardkv 服务当中，然后会做负载均衡，尽量让分片分的均匀一些。
leave: join的逆操作
move:移动分片到指定的group

shardkv是真正提供kv store服务对外.
shardkv client 来请求get put append操作..
shardkv server 来响应请求。
shardkv server 通过rpc来响应get put append 等操作，后台也有一些goroutine用来 维护一些元数据

1. applyroutine用来apply log
  - log 分为client 类和 config类
  - client 类有getputappend
  - config 类有 updateShardState  getshard和putshard
2. 后台有一个goroutine 定期 向controller请求 最新的config，拿到最新的config后会提交日志到raft层(updateShardState)
3. 后台定期检查是否有 分配到自己的shard但是本地还没有的，如果有需要根据config，向对应的group索要 
  - 向对应的group中的服务器发送rpc 请求shard
  - rpc携带对应数据返回，并提交log，applyroutine 进行对应的state修改(本地的shard变为Exist)
4. 后台定期检查 需要发送出去的shard是否发送到了
  - 对所有waitgive的shard ，向其所在的group发送rpc 
  - rpc返回收到则 提交giveshard到日志，applyroutine 负责修改对应的state为NotExist
5. 后台定期检查最新的log 的term是否与raft currentTerm相同，不同则需要添加一条emptylog.

```c
void applyroutine(){
  for{
    args = get_args();
    switch op{
      case get:
      case putappend:
        // do some logic
      case updateConfig:
        update_shard_state();
      case ShardGet:
        shard_state = Exist;
        shard = args.data;
        break;
      case ShardGive:
        shard_state = NotExist;
        shard = NULL;
        break;
      case EmptyLog:
        break;
    }
    send_reply();
  }
}

```
```

