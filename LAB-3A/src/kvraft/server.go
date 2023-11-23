package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
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

type SeqAndReply struct {
	seq      int32
	reply    string
	clientId int64
	index    int
}
type SeqAndIndex struct {
	seq   int32
	index int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	hasReqSeq      map[int64]SeqAndIndex //记录每个client发送请求的最大序号
	hasFinishedReq map[int64]SeqAndReply //记录每个server已经完成的请求（最大序列号）和相应的回复
	applyChanCond  *sync.Cond            //此条件变量用于 在applyChan取出一个元素并放置在队列中后用于通知正在等待的携程
	timeout        time.Duration         //超时时间
	data           map[string]string
}

func (kv *KVServer) GetNewestFinishedTask(method string, clerkId int64, reply interface{}) {
	//（这里始终返回最大请求号对应的回复，因为对于每个client来说，重新发送的请求对应的结果一定是之前最新的一个请求）
	switch method {
	case GET:
		reply.(*GetReply).Err = OK
		reply.(*GetReply).Value = kv.hasFinishedReq[clerkId].reply
	default:
		reply.(*PutAppendReply).Err = OK
	}
}

func (kv *KVServer) WaitingForApplyChanReply(method string, args interface{}, reply interface{}, index int) {
	DPrintf("Server-WaitingForApplyChanReply: index : %v\n", index)
	done := make(chan string)
	clerkId, seq, _, _ := kv.getArgsAttr(method, args)
	go func() {
		for {
			DPrintf("(%v) : Server-WaitingForApplyChanReply : start waiting\n", kv.me)
			kv.applyChanCond.Wait()
			DPrintf("(%v) : Server-WaitingForApplyChanReply : apply get result success\n", kv.me)
			_, isFinishExist := kv.hasFinishedReq[clerkId]
			//DPrintf("(%v) : isFinishExist : %v, index : %v, ClientId : %v\n", kv.me, isFinishExist, kv.hasFinishedReq[clerkId].index, clerkId)
			if isFinishExist {
				DPrintf("kv.hasFinishedReq[clerkId].index == index (%v) ; kv.hasFinishedReq[clerkId].seq == seq (%v)", kv.hasFinishedReq[clerkId].index == index, kv.hasFinishedReq[clerkId].seq == seq)
				if kv.hasFinishedReq[clerkId].index == index {
					if kv.hasFinishedReq[clerkId].seq == seq {
						done <- kv.hasFinishedReq[clerkId].reply
					} else {
						done <- ErrNoKey
					}
					return
				} else if kv.hasFinishedReq[clerkId].index > index {
					done <- ErrNoKey
					return
				}
			}
		}
	}()
	select {
	case res := <-done:
		DPrintf("Server-WaitingForApplyChanReply: res : %v\n", res)
		if res == ErrNoKey {
			kv.setReplyErr(method, reply, ErrNoKey)
		} else {
			kv.setReplyErr(method, reply, OK)
			if method == GET {
				reply.(*GetReply).Value = res
			}
		}
	case <-time.After(kv.timeout):
		DPrintf("(%v) : Server-WaitingForApplyChanReply: timeout\n", kv.me)
		kv.setReplyErr(method, reply, Timeout)
	}
}

func (kv *KVServer) setReplyErr(method string, reply interface{}, err Err) {
	switch method {
	case GET:
		reply.(*GetReply).Err = err
	default:
		reply.(*PutAppendReply).Err = err
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

func (kv *KVServer) GetAndPutAppendHandler(args interface{}, reply interface{}, method string) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.setReplyErr(method, reply, ErrWrongLeader)
		return
	}
	kv.mu.Lock()
	//双检锁，为了避免拿到锁后，已经不是leader了
	_, isLeader = kv.rf.GetState()
	if !isLeader {
		kv.setReplyErr(method, reply, ErrWrongLeader)
		kv.mu.Unlock()
		return
	}
	DPrintf("(%v) : is Leader, method : %v\n", kv.me, method)
	//获取参数信息
	clerkId, seq, key, value := kv.getArgsAttr(method, args)
	//判断当前客户端有没有请求过
	_, hasRequested := kv.hasReqSeq[clerkId]
	if !hasRequested {
		kv.hasReqSeq[clerkId] = SeqAndIndex{
			seq:   seq - 1,
			index: -1,
		}
	}
	//用来标记当前请求是否正在执行/过期请求/新的请求
	isProcessing := kv.hasReqSeq[clerkId].seq == seq
	isExpired := kv.hasReqSeq[clerkId].seq > seq
	isNewReq := kv.hasReqSeq[clerkId].seq < seq
	if isExpired {
		DPrintf("Server: request isExpired\n")
		//请求过时,直接返回最新的执行结果即可
		kv.GetNewestFinishedTask(method, clerkId, reply)
		kv.mu.Unlock()
		return
	} else if isProcessing {
		DPrintf("Server: request isProcessing\n")
		//请求正在执行或者执行完毕
		_, isExistFinished := kv.hasFinishedReq[clerkId]
		if !isExistFinished || kv.hasFinishedReq[clerkId].seq < seq {
			//请求未执行完毕，但是正在执行中，那么就没必要发送Start请求，仅仅需要等待即可
			kv.WaitingForApplyChanReply(method, args, reply, kv.hasReqSeq[clerkId].index)
			kv.mu.Unlock()
			return
		} else {
			//请求已经执行完毕，返回执行结果即可
			kv.GetNewestFinishedTask(method, clerkId, reply)
			kv.mu.Unlock()
			return
		}
	} else if isNewReq {
		DPrintf("Server: request isNewReq\n")
		//请求并未出现过，即新的请求
		kv.mu.Unlock()
		chanIndex, _, isLeader := kv.rf.Start(Op{OpType: method, Key: key, Value: value, Seq: seq, ClientId: clerkId})
		if !isLeader {
			kv.setReplyErr(method, reply, ErrWrongLeader)
			return
		}
		kv.mu.Lock()
		kv.hasReqSeq[clerkId] = SeqAndIndex{
			seq:   seq,
			index: chanIndex,
		}
		kv.WaitingForApplyChanReply(method, args, reply, chanIndex)
		kv.mu.Unlock()
		return
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("****** Server-Get *********\n")
	kv.GetAndPutAppendHandler(args, reply, GET)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("****** Server-PutAppend *********\n")
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
	kv.hasReqSeq = make(map[int64]SeqAndIndex)
	kv.data = make(map[string]string)
	kv.timeout = 500 * time.Millisecond
	kv.applyChanCond = sync.NewCond(&kv.mu)

	//将applyChan中的已经完成的任务取出，并放置在队列中
	go kv.checkApplyChan()
	return kv
}

/*
*
将applyChan中已经完成的任务放入recordList中，并告知所有的等待的线程
*/
func (kv *KVServer) checkApplyChan() {
	for {
		task := <-kv.applyCh
		kv.mu.Lock()
		index := task.CommandIndex
		content := task.Command.(Op)
		DPrintf("(%v): ------ checkApplyChan task %v complete --------\n", kv.me, task)
		DPrintf("------------- method : %v ---------------\n", content.OpType)
		DPrintf("------------- key : %v ---------------\n", content.Key)
		DPrintf("------------- value : %v ---------------\n", content.Value)
		DPrintf("------------- clientId : %v ---------------\n", content.ClientId)
		DPrintf("------------- seq : %v ---------------\n", content.Seq)
		//DPrintf("-------------------------------------------------------\n")

		_, isFinishedExist := kv.hasFinishedReq[content.ClientId]
		if !isFinishedExist || isFinishedExist && kv.hasFinishedReq[content.ClientId].seq < content.Seq {
			reply := ""
			switch content.OpType {
			case GET:
				_, exist := kv.data[content.Key]
				if !exist {
					reply = ErrNoKey
				} else {
					reply = kv.data[content.Key]
				}
			case PUT:
				kv.data[content.Key] = content.Value
			case APPEND:
				kv.data[content.Key] += content.Value
			}
			kv.hasFinishedReq[content.ClientId] = SeqAndReply{
				seq:      content.Seq,
				clientId: content.ClientId,
				index:    index,
				reply:    reply,
			}
		}
		kv.applyChanCond.Signal()
		//DPrintf("(%v) : ----------- Broadcast ----------- \n", kv.me)
		kv.mu.Unlock()
	}
}
