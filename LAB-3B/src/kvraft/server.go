package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"container/list"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
		//raft.PrintToFile(format, a...)
		//raft.Print2File(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
	Value    string
	ClientId int64
	Seq      int32
}

// SeqAndReply 最新的请求及结果
type SeqAndReply struct {
	seq   int32
	reply string
}

// SeqAndIndex 已经进行的请求对应的序列号以及预期的日志索引
type SeqAndIndex struct {
	seq   int32
	index int
}

// Node 对应于每个clientIdList的节点，即请求的clientId、时间撮、以及预期的日志索引
type Node struct {
	clientId      int64
	timestamp     time.Time
	expectedIndex int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//hasReqSeq      map[int64]SeqAndIndex //记录每个client发送请求的最大序号
	hasFinishedReq map[int64]SeqAndReply //记录每个server已经完成的请求（最大序列号）和相应的回复
	timeout        time.Duration         //超时时间
	data           map[string]string

	clientIdList    list.List                  //结合map实现LRU算法（每个节点存放的元素为：clientId、timestamp、ExpectedIndex）
	clientIdMap     map[int64]*list.Element    // 存储clientId对应的链表指针，实现O(1)的删除和移动
	clientIdChanMap map[int64]chan SeqAndReply // 为每个client建立一个chan用来接收数据，当然也是通过LRU算法来动态调整
}

func (kv *KVServer) GetNewestFinishedTask(method string, clerkId int64, reply interface{}) {
	//（这里始终返回最大请求号对应的回复，因为对于每个client来说，重新发送的请求对应的结果一定是之前最新的一个请求）
	switch method {
	case GET:
		if kv.hasFinishedReq[clerkId].reply == "" {
			reply.(*GetReply).Err = ErrNoKey
		} else {
			reply.(*GetReply).Err = OK
		}
		reply.(*GetReply).Value = kv.hasFinishedReq[clerkId].reply
	default:
		reply.(*PutAppendReply).Err = OK
	}
}

// 删除 kv.clientList 中的指定节点
func (kv *KVServer) delClientNode(nodePtr *list.Element) {
	//DPrintf("---------- delClientNode ------------")
	clientId := nodePtr.Value.(*Node).clientId
	delete(kv.clientIdMap, clientId)
	delete(kv.clientIdChanMap, clientId)
	kv.clientIdList.Remove(nodePtr)
}

func (kv *KVServer) checkTimeOutAndRemove() {
	now := time.Now()
	//DPrintf("checkTimeoutAndRemove enter\n")
	for kv.clientIdList.Len() != 0 {
		nodePtr := kv.clientIdList.Back()
		timeDiff := now.Sub((nodePtr.Value).(*Node).timestamp)
		if timeDiff > kv.timeout {
			kv.delClientNode(nodePtr)
			////DPrintf("暂时不删除")
			break
		} else {
			break
		}
	}
	//DPrintf("checkTimeoutAndRemove exit\n")
}

// 添加最新等待结果的cliendId，如果存在，则无需添加；否则添加，并且根据超时时间将最近未访问的节点删除
func (kv *KVServer) addNewClient(clientId int64, exceptIndex int) {
	_, isExistInClientIdMap := kv.clientIdMap[clientId]
	now := time.Now()
	//DPrintf("new channel --- clientId : %v, exceptIndex : %v\n", clientId, exceptIndex)
	if isExistInClientIdMap {
		//存在,则移动到链表头部（标记为最新访问的client）
		clientNodePtr := kv.clientIdMap[clientId]
		clientNodePtr.Value.(*Node).timestamp = now
		clientNodePtr.Value.(*Node).expectedIndex = exceptIndex
		kv.clientIdList.MoveToFront(clientNodePtr)
		kv.clientIdMap[clientId] = kv.clientIdList.Front()
	} else {
		//不存在
		nodePtr := kv.clientIdList.PushBack(&Node{
			clientId:      clientId,
			timestamp:     now,
			expectedIndex: exceptIndex,
		})
		kv.clientIdMap[clientId] = nodePtr
		kv.clientIdChanMap[clientId] = make(chan SeqAndReply, 1)
	}
	kv.checkTimeOutAndRemove()
	//DPrintf("addNewClient exit\n")
}

func (kv *KVServer) setReplyErr(method string, reply interface{}, err Err) {
	switch method {
	case GET:
		reply.(*GetReply).Err = err
	default:
		reply.(*PutAppendReply).Err = err
	}
}

func (kv *KVServer) setReply(method string, reply interface{}, value string) {
	switch method {
	case GET:
		reply.(*GetReply).Value = value
	}
}

func (kv *KVServer) getArgsAttr(method string, args interface{}) (clerkId int64, seq int32, key string, value string) {
	switch method {
	case GET:
		temp := args.(*GetArgs)
		clerkId = temp.ClerkId
		seq = temp.Seq
		key = temp.Key
		value = ""
	default:
		temp := args.(*PutAppendArgs)
		clerkId = temp.ClerkId
		seq = temp.Seq
		key = temp.Key
		value = temp.Value
	}
	return clerkId, seq, key, value
}

func (kv *KVServer) checkIsLeader(method string, reply interface{}) bool {
	//DPrintf("(%v) : checkIsLeader\n", kv.me)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.setReplyErr(method, reply, ErrWrongLeader)
		return false
	}
	return true
}

func (kv *KVServer) GetAndPutAppendHandler(args interface{}, reply interface{}, method string) {
	// Your code here.
	if isLeader := kv.checkIsLeader(method, reply); !isLeader {
		return
	}
	kv.mu.Lock()
	//双检锁，为了避免拿到锁后，已经不是leader了
	if isLeader := kv.checkIsLeader(method, reply); !isLeader {
		kv.mu.Unlock()
		return
	}
	DPrintf("(%v) : is Leader, method : %v\n", kv.me, method)

	//获取参数信息
	clerkId, seq, key, value := kv.getArgsAttr(method, args)
	_, isExistFinished := kv.hasFinishedReq[clerkId]
	if isExistFinished && kv.hasFinishedReq[clerkId].seq >= seq {
		//如果该任务已经完成，则直接返回结果即可
		//DPrintf("task has complete\n")
		kv.GetNewestFinishedTask(method, clerkId, reply)
		kv.mu.Unlock()
		return
	}

	//该任务未执行，则发起写日志请求即可
	//DPrintf("task is new\n")
	kv.mu.Unlock()
	chanIndex, _, isLeader := kv.rf.Start(Op{OpType: method, Key: key, Value: value, Seq: seq, ClientId: clerkId})
	DPrintf("call Start success --- index : %v\n", chanIndex)
	if !isLeader {
		kv.setReplyErr(method, reply, ErrWrongLeader)
		return
	}
	if isLeader = kv.checkIsLeader(method, reply); !isLeader {
		return
	}
	kv.mu.Lock()
	kv.addNewClient(clerkId, chanIndex)
	clientChan := kv.clientIdChanMap[clerkId]
	kv.mu.Unlock()
	//DPrintf("(%v) : waiting 。。。。。。 key : %v, value: %v\n", kv.me, key, value)
	select {
	case res := <-clientChan:
		if res.seq == seq {
			if method == GET && res.reply == "" {
				kv.setReplyErr(method, reply, ErrNoKey)
			} else {
				kv.setReplyErr(method, reply, OK)
			}
			kv.setReply(method, reply, res.reply)
		} else {
			kv.setReplyErr(method, reply, ErrNoKey)
		}
	case <-time.After(kv.timeout):
		//DPrintf("(%v) : clientId : %v, timeout expectedIndex : %v\n", kv.me, clerkId, chanIndex)
		kv.mu.Lock()
		kv.setReplyErr(method, reply, Timeout)
		if nodePtr, isExist := kv.clientIdMap[clerkId]; isExist {
			nodePtr.Value.(*Node).expectedIndex = -1
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//DPrintf("****** Server-Get *********\n")
	kv.GetAndPutAppendHandler(args, reply, GET)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("****** Server-PutAppend *********\n")
	kv.GetAndPutAppendHandler(args, reply, args.Op)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// 初始化
	kv.hasFinishedReq = make(map[int64]SeqAndReply)
	//kv.hasReqSeq = make(map[int64]SeqAndIndex)
	kv.data = make(map[string]string)
	kv.timeout = 500 * time.Millisecond

	kv.clientIdList = list.List{}
	kv.clientIdMap = make(map[int64]*list.Element)
	kv.clientIdChanMap = make(map[int64]chan SeqAndReply)

	//将applyChan中的已经完成的任务取出，并放置在队列中
	go kv.checkApplyChan()
	return kv
}

/*
*
sendLog2ClientChan - 收到raft层所得到的日志信息，通过管道通知等待结果的其他协程
*/
func (kv *KVServer) sendLog2ClientChan(content Op, index int) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	_, isExistInClientIdChanMap := kv.clientIdChanMap[content.ClientId]
	if isExistInClientIdChanMap {
		nodePtr := kv.clientIdMap[content.ClientId]
		////DPrintf("(%v): index1 : %v, index2 : %v\n", kv.me, nodePtr.Value.(*Node).expectedIndex, index)
		if nodePtr.Value.(*Node).expectedIndex == index {
			////DPrintf("(%v) : SUCCESS  seq : %v reply : %v\n", kv.me, kv.hasFinishedReq[content.ClientId].seq, kv.hasFinishedReq[content.ClientId].reply)
			kv.clientIdChanMap[content.ClientId] <- kv.hasFinishedReq[content.ClientId]
		}
	}
}

/*
*
commandHandler - checkApplyChan接收到普通日志同步的信息后，调用此函数来实现kvServer的更新
*/
func (kv *KVServer) commandHandler(index int, content Op) {
	_, isFinishedExist := kv.hasFinishedReq[content.ClientId]
	if !isFinishedExist || (isFinishedExist && (kv.hasFinishedReq[content.ClientId].seq < content.Seq)) {
		reply := ""
		switch content.OpType {
		case GET:
			if _, exist := kv.data[content.Key]; exist {
				reply = kv.data[content.Key]
			}
		case PUT:
			kv.data[content.Key] = content.Value
		case APPEND:
			kv.data[content.Key] += content.Value
		}
		kv.hasFinishedReq[content.ClientId] = SeqAndReply{
			seq:   content.Seq,
			reply: reply,
		}

		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
			//日志过长，需要发起快照请求
			w := new(bytes.Buffer)
			encoder := gob.NewEncoder(w)
			encoder.Encode(kv.data)
			encoder.Encode(kv.hasFinishedReq)
			data := w.Bytes()
			kv.rf.Snapshot(index, data)
		}
		kv.sendLog2ClientChan(content, index)
	}
}

/*
*
snapShotHandler - checkApplyChan接收到快照同步的信息后，调用此函数来实现kvServer的更新
*/
func (kv *KVServer) snapShotHandler(snapshot []byte, index int) {
	kv.data = make(map[string]string)
	kv.hasFinishedReq = make(map[int64]SeqAndReply)
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	err := d.Decode(&kv.data)
	if err != nil {
		return
	}
	err = d.Decode(&kv.hasFinishedReq)
	if err != nil {
		return
	}
}

/*
*checkApplyChan 将applyChan中已经完成的任务放入recordList中，并告知所有的等待的线程
 */
func (kv *KVServer) checkApplyChan() {
	for {
		task := <-kv.applyCh
		kv.mu.Lock()
		if task.CommandValid {
			// 非快照,普通日志同步
			index := task.CommandIndex
			content := task.Command.(Op)
			kv.commandHandler(index, content)
		} else {
			//快照同步
			kv.snapShotHandler(task.Snapshot, task.SnapshotIndex)
		}
		kv.mu.Unlock()
	}
}
