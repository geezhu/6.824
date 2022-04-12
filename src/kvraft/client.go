package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	session *clientSession
	ApplyCh chan ClientApplyMsg
	// You will have to modify this struct.
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
	ck.ApplyCh = make(chan ClientApplyMsg)
	ck.session = MakeClientSession(ck.ApplyCh, servers)
	return ck
}

//
// fetch the current value for a Key.
// returns "" if the Key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	RetCh := make(chan ClientReply)
	// You will have to modify this function.
	//DPrintf("GEt")
	ck.ApplyCh <- ClientApplyMsg{
		Key:     key,
		Value:   "",
		Op:      "Get",
		retChan: RetCh,
	}
	cr := <-RetCh
	if cr.Err == ErrNoKey {
		DPrintf("Get(%v)=%v", key, "")
		return ""
	} else if cr.Err == OK {
		DPrintf("Get(%v)=%v", key, cr.Value)
		return cr.Value
	}
	panic("Wrong Return Err！")
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//DPrintf("PA")
	RetCh := make(chan ClientReply)
	ck.ApplyCh <- ClientApplyMsg{
		Key:     key,
		Value:   value,
		Op:      op,
		retChan: RetCh,
	}
	cr := <-RetCh
	time.Sleep(time.Duration(1) * time.Millisecond)
	if cr.Err == OK {
		return
	}

	//panic("Wrong Return Err！")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
