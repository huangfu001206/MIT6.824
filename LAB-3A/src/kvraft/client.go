package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "time"

const (
	Failed = "Failed"
)

type GetRespType struct {
	status  string
	context string
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seq             atomic.Int32 //标记最新的请求序号
	clerkId         int64        // 用拉标记自身的唯一id
	leaderIndex     int          //用于标记上一个请求的对象索引（大概率为leader）
	timeoutDuration time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.seq.Store(0)
	ck.leaderIndex = -1
	ck.timeoutDuration = 5 * time.Second
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	if ck.leaderIndex == -1 {
		ck.leaderIndex = 0
	}
	numServer := len(ck.servers)
	args := GetArgs{
		Key:     key,
		Seq:     ck.seq.Load(),
		ClerkId: ck.clerkId,
	}
	reply := GetReply{}
	for {
		msgChan := make(chan GetRespType)
		go ck.sendGetReq(&args, &reply, ck.leaderIndex, msgChan)
		select {
		case msg := <-msgChan:
			switch msg.status {
			case OK:
				ck.seq.Store(ck.seq.Load() + 1)
				return msg.context
			}
		case <-time.After(ck.timeoutDuration):

		}
		ck.leaderIndex = (ck.leaderIndex + 1) % numServer
	}
}

func (ck *Clerk) sendGetReq(args *GetArgs, reply *GetReply, leaderIndex int, msgChan chan GetRespType) {
	ok := ck.servers[leaderIndex].Call("KVServer.Get", &args, &reply)
	if !ok {
		msgChan <- GetRespType{
			status: Failed,
		}
	} else {
		msgChan <- GetRespType{
			status:  string(reply.Err),
			context: reply.Value,
		}
	}
}

func (ck *Clerk) sendPutReq(args *PutAppendArgs, reply *PutAppendReply, leaderIndex int, msgChan chan string) {
	ok := ck.servers[leaderIndex].Call("KVServer.PutAppend", &args, &reply)
	if !ok {
		msgChan <- Failed
	} else {
		msgChan <- string(reply.Err)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if ck.leaderIndex == -1 {
		ck.leaderIndex = 0
	}
	numServer := len(ck.servers)

	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.clerkId,
		Seq:     ck.seq.Load(),
	}
	reply := PutAppendReply{}
	for {
		msgChan := make(chan string)
		//异步发送Put Rpc请求
		go ck.sendPutReq(&args, &reply, ck.leaderIndex, msgChan)
		//接收结果及超时检测
		select {
		case msg := <-msgChan:
			if msg == OK {
				ck.seq.Store(ck.seq.Load() + 1)
				return
			}
		case <-time.After(ck.timeoutDuration):
			DPrintf("(%v) : PutAppend Timeout", ck.clerkId)
		}
		ck.leaderIndex = (ck.leaderIndex + 1) % numServer
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
