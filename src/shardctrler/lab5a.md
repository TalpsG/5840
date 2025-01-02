# sharded key value service
## controller
lab5最终会实现一个multi raft的对外的kv store. 
5a实现的是其中的controller部分。
controller实际上也是一个上层使用之前编写的raft的服务，只不过现在我们提供的不是kv服务，而是存储整个kv store的配置信息的服务。
group:一组server组成的集群，由raft协议保证一致性。
shard:将全部keyvalue分摊到各个group上，这样可以提高整体的 吞吐量。

join，添加一个group到集群.
leave，删除一个group
move，将一个shard放到指定的group上
query，查看某个config


join: 在config当中添加一个对应的映射到map即可，添加过后需要rebalance进行负载均衡。
leave: join的逆操作
move: 修改`shard[shardidx]`对应的gid即可
query:...

比较麻烦的是rebalance过程，实现起来繁琐一些不过思路不难.
rebalance的目标是要以较小的调整实现负载均衡，换句话说就是重新分配，group上shard多的就把多的拿掉放到其他group上，但是不能一下全拿走然后重新分配。

