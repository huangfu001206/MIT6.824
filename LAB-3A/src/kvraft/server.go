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

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
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

func (kv *KVServer) GetFinishedResponse(clerkId int64, reply *GetReply) {
	//（这里始终返回最大请求号对应的回复，因为对于每个client来说，重新发送的请求对应的结果一定是之前最新的一个请求）
	reply.Err = OK
	reply.Value = kv.hasFinishedReq[clerkId].reply
}

func (kv *KVServer) WaitingForApplyChanReply(args *GetArgs, reply *GetReply, index int) {
	done := make(chan string)
	go func() {
		for {
			kv.applyChanCond.Wait()
			_, isFinishExist := kv.hasFinishedReq[args.ClerkId]
			if isFinishExist && kv.hasFinishedReq[args.ClerkId].index == index {
				if kv.hasFinishedReq[args.ClerkId].seq == args.Seq {
					done <- kv.hasFinishedReq[args.ClerkId].reply
				} else {
					done <- ErrNoKey
				}
				return
			}
		}
	}()
	select {
	case res := <-done:
		if res == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = res
		}
	case <-time.After(kv.timeout):
		reply.Err = Timeout
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	//双检锁，为了避免拿到锁后，已经不是leader了
	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//判断当前客户端有没有请求过
	_, hasRequested := kv.hasReqSeq[args.ClerkId]
	if !hasRequested {
		kv.hasReqSeq[args.ClerkId] = SeqAndIndex{
			seq:   args.Seq - 1,
			index: -1,
		}
	}
	//用来标记当前请求是否正在执行/过期请求/新的请求
	isProcessing := kv.hasReqSeq[args.ClerkId].seq == args.Seq
	isExpired := kv.hasReqSeq[args.ClerkId].seq > args.Seq
	isNewReq := kv.hasReqSeq[args.ClerkId].seq < args.Seq
	if isExpired {
		//请求过时,直接返回最新的执行结果即可
		kv.GetFinishedResponse(args.ClerkId, reply)
		kv.mu.Unlock()
		return
	} else if isProcessing {
		//请求正在执行或者执行完毕
		_, isExistFinished := kv.hasFinishedReq[args.ClerkId]
		if !isExistFinished || kv.hasFinishedReq[args.ClerkId].seq < args.Seq {
			//请求未执行完毕，但是正在执行中，那么就没必要发送Start请求，仅仅需要等待即可
			kv.WaitingForApplyChanReply(args, reply, kv.hasReqSeq[args.ClerkId].index)
		} else {
			//请求已经执行完毕，返回执行结果即可
			kv.GetFinishedResponse(args.ClerkId, reply)
			kv.mu.Unlock()
			return
		}
	} else if isNewReq {
		//请求并未出现过，即新的请求
		key := args.Key
		kv.mu.Unlock()
		chanIndex, _, isLeader := kv.rf.Start(Op{OpType: "Get", Key: key, Value: "", Seq: args.Seq})
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		kv.hasReqSeq[args.ClerkId] = SeqAndIndex{
			seq:   args.Seq,
			index: chanIndex,
		}
		kv.WaitingForApplyChanReply(args, reply, chanIndex)
		kv.mu.Unlock()
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
	kv.timeout = 100 * time.Millisecond
	kv.applyChanCond = sync.NewCond(&sync.Mutex{})

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
		_, isFinishedExist := kv.hasFinishedReq[content.ClientId]
		if isFinishedExist && kv.hasFinishedReq[content.ClientId].seq < content.Seq {
			reply := ""
			switch content.OpType {
			case "Get":
				_, exist := kv.data[content.Key]
				if !exist {
					reply = ErrNoKey
				} else {
					reply = kv.data[content.Key]
				}
			case "Put":
				kv.data[content.Key] = content.Value
			case "Append":
				kv.data[content.Key] += content.Value
			}
			kv.hasFinishedReq[content.ClientId] = SeqAndReply{
				seq:      content.Seq,
				clientId: content.ClientId,
				index:    index,
				reply:    reply,
			}
		}
		kv.mu.Unlock()
		kv.applyChanCond.Broadcast()
	}
}
