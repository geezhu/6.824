package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func logger(format string, a ...interface{}) (n int, err error) {
	//pc, _, line, _ := runtime.Caller(1)
	//caller:=runtime.FuncForPC(pc).Name()
	//callers:=strings.Split(caller, ".")
	//caller="[From:"+callers[len(callers)-1]+", line="+strconv.Itoa(line)+"]"
	//if Debug {
	//	log.Printf(format+caller, a...)
	//}
	return
}

type operation int

const (
	getOp    operation = iota
	putOp    operation = iota
	appendOp operation = iota
)

type Op struct {
	Id  uid
	Seq int
	Rcv int
	Op  operation
	Key string
	Val string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu           sync.Mutex
	session_mu   sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	Buffer       map[string]string
	SessionMap   map[uid]int
	SessionArr   []session
	// Your definitions here.
}

//func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
//	// Your code here.
//}
//
//func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}
func (kv *KVServer) AppendOp(args *AppendOpArgs, reply *AppendOpReply) {
	//DPrintf("KV.AppendOp")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.IsLeader() {
		reply.NotLeader = false
	} else {
		reply.NotLeader = true
		return
	}
	if _, ok := kv.SessionMap[args.Uid]; ok {

	} else {
		kv.SessionMap[args.Uid] = len(kv.SessionArr)
		kv.SessionArr = append(kv.SessionArr, *MakeSession(args.Uid, 0, 0))
	}
	i := kv.SessionMap[args.Uid]
	kv.SessionArr[i].AppendOp(&kv.session_mu, kv.rf, args, reply)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *KVServer) InstallSnapshot(snap []byte) {
	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)
	var buf map[string]string
	var SessionMap map[uid]int
	var SessionArr []session
	if d.Decode(&buf) != nil ||
		d.Decode(&SessionMap) != nil ||
		d.Decode(&SessionArr) != nil {
		panic("readPersist:Decode Err!")
	} else {
		kv.SessionMap = SessionMap
		kv.SessionArr = SessionArr
		kv.Buffer = buf
	}
}
func (kv *KVServer) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.Buffer)
	_ = e.Encode(kv.SessionMap)
	_ = e.Encode(kv.SessionArr)
	return w.Bytes()
}
func (kv *KVServer) execute(op1 Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := ClientResult{
		Seq: op1.Seq,
		Err: "",
		Val: "",
	}
	originalVal, containsKey := kv.Buffer[op1.Key]
	sessionIndex, containSession := kv.SessionMap[op1.Id]
	if !containSession {
		s1 := MakeSession(op1.Id, 0, op1.Seq)
		kv.SessionMap[op1.Id] = len(kv.SessionArr)
		kv.SessionArr = append(kv.SessionArr, *s1)
		sessionIndex = kv.SessionMap[op1.Id]
	}
	if !kv.SessionArr[sessionIndex].isExecutable(&kv.session_mu, op1.Seq) {
		return
	}
	DPrintf("ApplySeq[%v]:[%v]=%v", op1.Id, kv.me, kv.SessionArr[sessionIndex].ApplySeq)
	switch op1.Op {
	case putOp:
		kv.Buffer[op1.Key] = op1.Val
		res.Err = OK
		kv.SessionArr[sessionIndex].putWrite(&kv.session_mu, op1.Seq)
		DPrintf("execute[%v,%v]:[%v]buf[%v]=%v", op1.Seq, op1.Id, kv.me, op1.Key, kv.Buffer[op1.Key])
	case appendOp:
		kv.Buffer[op1.Key] = originalVal + op1.Val
		DPrintf("execute[%v,%v]:[%v]buf[%v]=%v", op1.Seq, op1.Id, kv.me, op1.Key, kv.Buffer[op1.Key])
		res.Err = OK
		kv.SessionArr[sessionIndex].putWrite(&kv.session_mu, op1.Seq)
	case getOp:
		if containsKey {
			res.Val = originalVal
			DPrintf("execute[%v,%v]:[%v]buf[%v]=%v", op1.Seq, op1.Id, kv.me, op1.Key, kv.Buffer[op1.Key])
			res.Err = OK
		} else {
			res.Err = ErrNoKey
			DPrintf("execute[%v,%v]:[%v]buf[%v]=NoKey", op1.Seq, op1.Id, kv.me, op1.Key)
		}
		kv.SessionArr[sessionIndex].putRead(&kv.session_mu, res, op1.Rcv)
	}

}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) apply() {
	for kv.killed() == false {
		count := 0
		for msg := range kv.applyCh {
			//DPrintf("%v:RECV ApplyMsg",kv.me)
			if msg.CommandValid {
				count += 1
				if msg.Command == nil {
					//noop
				} else if val, ok := msg.Command.(Op); ok {
					//DPrintf("Exec=%v",val.Seq)
					kv.execute(val)
				} else {
					panic("wrong type of cmd")
				}
				if kv.maxraftstate != -1 && count*(60) >= kv.maxraftstate {
					count = 0
					DPrintf("[%v]Now Snapshot from [%v]!", kv.me, msg.CommandIndex)
					kv.rf.Snapshot(msg.CommandIndex, kv.MakeSnapshot())
				}
			} else if msg.SnapshotValid == true {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.InstallSnapshot(msg.Snapshot)
					count = 0
					DPrintf("[%v]Suc install Snapshot!", kv.me)
				} else {
					DPrintf("[%v]Not install Snapshot!", kv.me)
				}
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
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
	kv.SessionArr = make([]session, 0)
	kv.SessionMap = make(map[uid]int)
	kv.Buffer = make(map[string]string)
	// You may need initialization code here.
	go kv.apply()
	return kv
}
