package kvraft

import (
	"6.5840/labrpc"
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
	seq             int32 //标记最新的请求序号
	clerkId         int64 // 用拉标记自身的唯一id
	leaderIndex     int   //用于标记上一个请求的对象索引（大概率为leader）
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
	ck.seq = 0
	ck.leaderIndex = -1
	ck.timeoutDuration = 1 * time.Second
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
	DPrintf("Client-Get : key : %v, op : %v\n", key, "Get")
	// You will have to modify this function.
	if ck.leaderIndex == -1 {
		ck.leaderIndex = 0
	}
	numServer := len(ck.servers)
	args := GetArgs{
		Key:     key,
		Seq:     ck.seq,
		ClerkId: ck.clerkId,
	}
	reply := GetReply{}
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.Get", &args, &reply)
		if ok {
			DPrintf("Client-PutAppend : reply : %v", reply)
			if reply.Err == OK {
				ck.seq++
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.seq++
				return ""
			}
		}
		ck.leaderIndex = (ck.leaderIndex + 1) % numServer
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

	DPrintf("Client-PutAppend : key : %v, value : %v, op : %v\n", key, value, op)

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
		Seq:     ck.seq,
	}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("Client-PutAppend : reply : %v", reply)
		if ok && reply.Err == OK {
			ck.seq++
			return
		}
		ck.leaderIndex = (ck.leaderIndex + 1) % numServer
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Client-Put: key : %v , value : %v\n", key, value)
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("Client-Append: key : %v , value : %v\n", key, value)
	ck.PutAppend(key, value, APPEND)
}
