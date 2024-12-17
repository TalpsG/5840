# log compaction
由于每次重启机器后都要重新apply log，如果leader挂了可能还需要重新把所有log传给重启的节点。这样的方式显然需要优化，snapshot就是办法，大致思路就是保存快照。快照之前的log都不要了。这样重启后恢复快照即可，其实有点像db的checkpoint？


# section 7
snapshot 将当前的状态整个写入持久存储，zookeeper 和 chubby都是这样做的。

增量的方式去压缩有log cleanning 和lsmtree的方式，增量的方法可以将compaction的负载均摊到平时。


图12 展示了snapshotting实现的基本思路，每个server 有自己的 snapshots.
snapshot 覆盖了 已经提交过的部分日志.

snapshot 也需要有些metadata like lastincludeidx and lastincludeterm 用于appendentries来进行日志复制


leader必须偶尔发送snapshots到那些落下的follower(lag behind)，这是让这些follower快速跟上的办法(至少比rpcrpcrpcrpcrpc强，而且snapshot不见得比log大的奥)

leader用installSnapshot rpc来向follower发送snapshot。follower需要根据snapshot来判断自己的log是否还需要保留，通常情况下snapshot会比本地的log还要多，以至于多了一些没有commit的log，这种情况下直接删除掉自己的全部log即可。如果snapshot没有自己的log多，删除掉前面 与snapshot重合的log，保留后面的部分。

**感觉这里的leader发送给follower的snapshot必须只包含commit过的log，否则的话follower的snapshot是没有办法回退的，对不对呢？**


两个会影响snapshot性能的因素
1. 何时snapshot，常见的做法是log到大小到一个固定值？
2. snapshot耗时，可能会影响到服务的响应。可以采用copy on write的方式来进行snapshot，快照结束时进行删除日志。

# lab3d

lab3d 中 tester周期的调用这个函数
`Snapshot(index int,snapshot []byte)`
index表示这个快照的内包含的最大的logidx的内容.

同样需要实现installSnapshot这个函数。


## tips
1. 由于snapshot后会清除某个位置以前的所有log，因此需要记录这个位置，并且在voterequest比较idx和appendentries 日志复制时不能直接用idx了，需要减去 snapshot_idx
2. 删除log的时候不能直接按照idx删除，要删除上次snapshot_idx(已经删除)和本次snapshot_idx之间的log


## 思路
1. snapshot api 用于 将状态机的 snapshot保存下来，然后调用persist持久化一下(persist 只要需要持久化的数据变了就需要调用).
2. installSnapshot (leader) ,server在某个条件下(比如match落后100条的时候？)发送installSnapshot到 follower，然后follower同步
3. installSnapshot (follower) ,接收到snapshot，保存到raft内，并且向 状态机提交(applyCh)

## 遇到的问题
1. 向applych发送msg时不能持有锁，因为在上层的代码中 会根据apply的log个数定期 进行snapshot操作，snapshot api是咱们自己实现的，内部有锁，如果applych时有锁，会导致snapshot lock阻塞，该节点就死了
2. applier应该是要apply snapshot和普通的log，但是如果是statemachine传来的snapshot实际上是不需要apply的，此处是判断了snapshotidx和上次的snapshot_idx相不相同，不相同则代表有新的snapshot需要apply。该方法忽略了刚刚提到的情况，因此可能需要重复提交snapshot以及之后的log。增加一个flag。
3. installSnapshot 也充当hb的功能，因此将hb处修改为发送installsnapshot或者appendentries rpc
4. applysnapshot以及Snapshot api处都将log进行了裁剪

