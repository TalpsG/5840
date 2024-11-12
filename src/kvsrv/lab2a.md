# 无网络故障的kv服务
## server
server端直接加锁访问数据即可。client端的kv访问在server端就是串行的了.

## client
没有网络故障，所以直接调用rpc即可。

# 网络有故障
网络有故障，则client在rpc失败时并不知道server端的执行情况，因此需要一种机制来保证客户端的一次请求调用的多次rpc在server端也只对 数据操作一次。

server需要存储每一次操作的结果，服务长时间运行时可能会带来一定的存储压力，需要定期释放存储的结果。

## get
get的时候直接循环调用rpc 的get即可，因为get操作并不修改server端的数据。

## put get
在put和append的时候需要增加两个机制
1. opid，操作id。通过opid server可以判断 该操作是否已经做过了，对于已经操作过的op，存储op的结果。这样如果client端rpc返回的是false，再次调用时 server还是可以返回对应的结果
2. rpc返回后需要第二次rpc来告知server端该操作完成了，请删除结果吧，以后也用不上了

