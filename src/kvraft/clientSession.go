package kvraft

import (
	"6.824/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const InitElectionTimeout = 1000
const SENDINTVL = 1

type clientSession struct {
	id              uid
	clientCh        chan ClientApplyMsg
	sequentialMaker Sequential
	servers         []*labrpc.ClientEnd
	Ops             []ClientOp
	retchan         map[int]chan ClientReply
	//getSeq  []int
	retChanMu sync.Mutex
	rcv       int
	sendSeq   int
	killed    int32
	leader    int
}

func MakeClientSession(ApplyCh chan ClientApplyMsg, servers []*labrpc.ClientEnd) *clientSession {
	se := &clientSession{
		id:       randuid(20),
		clientCh: ApplyCh,
		sequentialMaker: Sequential{
			seq: 0,
			mu:  sync.Mutex{},
		},
		servers: servers,
		Ops:     make([]ClientOp, 0),
		retchan: make(map[int]chan ClientReply),
		//getSeq: 		 make([]int,0),
		retChanMu: sync.Mutex{},
		rcv:       -1,
		sendSeq:   -1,
		killed:    0,
		leader:    rand.Intn(len(servers)),
	}
	go se.getClientRequest()
	go se.send()
	return se
}
func (cs *clientSession) getClientRequest() {
	for ca := range cs.clientCh {
		//DPrintf("getClientRequest:ReCV Req")
		op1 := ClientOp{
			Seq: cs.sequentialMaker.GetSeq(),
			Key: ca.Key,
			Val: ca.Value,
			Op:  ca.Op,
		}
		cs.retChanMu.Lock()
		cs.retchan[op1.Seq] = ca.retChan
		cs.retChanMu.Unlock()
		cs.Ops = append(cs.Ops, op1)
	}
	DPrintf("Killed")
	atomic.CompareAndSwapInt32(&cs.killed, 0, 1)
}
func (cs *clientSession) send() {
	time.Sleep(time.Duration(InitElectionTimeout) * time.Millisecond)
	const MAX_NotProgress = 40
	NotProgress := 0
	oldrcv := 0
	oldldr := 0
	for atomic.LoadInt32(&cs.killed) == 0 {
		oldrcv = cs.rcv
		oldldr = cs.leader
		cs.sendmain()
		if cs.rcv == oldrcv && oldldr == cs.leader && cs.rcv < cs.sendSeq {
			NotProgress += 1
		} else {
			NotProgress = 0
		}
		if NotProgress >= MAX_NotProgress {
			cs.leader = (cs.leader + 1) % len(cs.servers)
			DPrintf("[%v]no progress,change ldr=%v", cs.id, cs.leader)
			NotProgress = 0
		}
		time.Sleep(time.Duration(SENDINTVL) * time.Millisecond)
	}
}
func (cs *clientSession) sendmain() {
	//DPrintf("Sendmain")
	send := -1
	for index := range cs.Ops {
		if cs.Ops[index].Seq == cs.sendSeq+1 {
			send = index
			break
		}
	}
	var sendOps []ClientOp
	if send == -1 {
		sendOps = make([]ClientOp, 0)
	} else {
		sendOps = cs.Ops[send:]
	}
	aoa := AppendOpArgs{
		Uid: cs.id,
		Rcv: cs.rcv,
		Ops: sendOps,
	}
	aor := new(AppendOpReply)
	retry := 0
	for cs.sendAppendOp(cs.leader, &aoa, aor) != true {
		retry += 1
		aor = new(AppendOpReply)
		if retry >= MAX_RETRY {
			cs.leader = (cs.leader + 1) % len(cs.servers)
			DPrintf("[%v]fail,change ldr=%v", cs.id, cs.leader)
			return
		}
	}
	//remove all the ops after rcv
	if aor.NotLeader {
		cs.leader = (cs.leader + 1) % len(cs.servers)
		DPrintf("[%v]not ldr,change ldr=%v", cs.id, cs.leader)
		return
	}
	startIndex := cs.rcv
	if aor.LastResultIndex > cs.rcv {
		DPrintf("res=%v", aor.LastResultIndex)
		cs.rcv = aor.LastResultIndex
	}
	discard := -1
	for index := range cs.Ops {
		if cs.Ops[index].Seq == cs.rcv {
			discard = index
		}
	}
	if discard != -1 {
		cs.Ops = cs.Ops[discard+1:]
	}
	if startIndex < cs.rcv {
		index2 := 0
		for i := startIndex + 1; i <= cs.rcv; i++ {
			if index2 < len(aor.Results) && aor.Results[index2].Seq == i {
				cs.retchan[i] <- ClientReply{
					Err:   aor.Results[index2].Err,
					Value: aor.Results[index2].Val,
				}
				DPrintf("recv=%v from %v", aor.Results[index2], cs.leader)
				cs.retChanMu.Lock()
				delete(cs.retchan, i)
				cs.retChanMu.Unlock()
				index2 += 1
			} else if _, ok := cs.retchan[i]; ok {
				cs.retchan[i] <- ClientReply{
					Err:   OK,
					Value: "",
				}
				cs.retChanMu.Lock()
				delete(cs.retchan, i)
				cs.retChanMu.Unlock()
			}
		}
	}
	cs.sendSeq = aor.Ack - 1
}
func (cs *clientSession) sendAppendOp(ldr int, args *AppendOpArgs, reply *AppendOpReply) bool {
	ok := cs.servers[ldr].Call("KVServer.AppendOp", args, reply)
	return ok
}
