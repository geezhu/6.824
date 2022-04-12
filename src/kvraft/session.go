package kvraft

import (
	"6.824/raft"
	"sync"
)

type session struct {
	Uid      uid
	Res      []ClientResult
	Term     int
	CurOp    []Op
	SendSeq  int
	AckSeq   int //nextSeq need to be sent by client
	ApplySeq int
}

func MakeSession(id uid, Term int, startSeq int) *session {
	se := &session{
		Uid:      id,
		Res:      make([]ClientResult, 0),
		Term:     Term,
		CurOp:    make([]Op, 0),
		SendSeq:  startSeq - 1,
		AckSeq:   startSeq,
		ApplySeq: startSeq - 1,
	}
	return se
}

//func (se *session)isDuplicate(seq int)bool{
//	//如果在applySeq之前算dup
//	return seq<=se.ApplySeq
//}

func (se *session) isExecutable(mu *sync.Mutex, seq int) bool {
	//如果在applySeq之前算dup
	mu.Lock()
	defer mu.Unlock()
	return se.ApplySeq+1 == seq
}
func (se *session) reset(term int) {
	//DPrintf("session reset")
	se.Term = term
	se.CurOp = make([]Op, 0)
	se.SendSeq = se.ApplySeq
	se.AckSeq = se.ApplySeq + 1
	se.ApplySeq = se.ApplySeq
}
func (se *session) removeResult(rcv int) {
	req := rcv + 1
	for i3 := range se.Res {
		if se.Res[i3].Seq < req {
			if i3 == len(se.Res)-1 {
				se.Res = se.Res[i3+1:]
				break
			}
		} else {
			se.Res = se.Res[i3:]
			break
		}
	}
}
func (se *session) putRead(mu *sync.Mutex, res ClientResult, rcv int) {
	mu.Lock()
	defer mu.Unlock()
	if len(se.Res) == 0 {
		se.Res = append(se.Res, res)
		goto end1
	}
	for index := range se.Res {
		if res.Seq < se.Res[index].Seq {
			if index == 0 {
				se.Res = append([]ClientResult{res}, se.Res...)
			} else {
				se.Res = append(se.Res[:index], append([]ClientResult{res}, se.Res[index:]...)...)
			}
			break
		}
	}
	if res.Seq > se.Res[len(se.Res)-1].Seq {
		se.Res = append(se.Res, res)
	}
end1:
	if res.Seq == se.ApplySeq+1 {
		se.ApplySeq += 1
		//DPrintf("82,applySeq=%v",se.ApplySeq)
	} else {
		panic("putRead:wrong order")
	}
	se.removeResult(rcv)
}
func (se *session) putWrite(mu *sync.Mutex, seq int) {
	mu.Lock()
	defer mu.Unlock()
	if seq == se.ApplySeq+1 {
		se.ApplySeq += 1
		//DPrintf("91,applySeq=%v",se.ApplySeq)
	} else {
		panic("putWrite:wrong order")
	}
}
func (se *session) putOp(rf *raft.Raft, Ops []ClientOp, rcv int) ([]ClientResult, int) {
	//check whether in CurOp or less than ackSeq
	index1 := 0
	index2 := 0
	//lambda for converting clientOp to Op
	convertToOp := func(op ClientOp) *Op {
		var op1 operation
		if op.Op == "Put" {
			op1 = putOp
		}
		if op.Op == "Get" {
			op1 = getOp
		}
		if op.Op == "Append" {
			op1 = appendOp
		}
		return &Op{
			Id:  se.Uid,
			Seq: op.Seq,
			Rcv: rcv,
			Op:  op1,
			Key: op.Key,
			Val: op.Val,
		}
	}
	tmp := make([]Op, 0)
	for i := range Ops {
		if Ops[i].Seq < se.SendSeq {
			index2++
		} else {
			break
		}
	}
	for i := 0; index1 < len(se.CurOp) && index2 < len(Ops); i++ {
		if se.CurOp[index1].Seq > Ops[index2].Seq {
			tmp = append(tmp, *convertToOp(Ops[index2]))
			index2++
		} else if se.CurOp[index1].Seq == Ops[index2].Seq {
			tmp = append(tmp, se.CurOp[index1])
			index1++
			index2++
		} else if se.CurOp[index1].Seq < Ops[index2].Seq {
			tmp = append(tmp, se.CurOp[index1])
			index1++
		}
	}
	if index1 == len(se.CurOp) {
		for ; index2 < len(Ops); index2++ {
			tmp = append(tmp, *convertToOp(Ops[index2]))
		}
	} else if index1 < len(se.CurOp) {
		tmp = append(tmp, se.CurOp[index1:]...)
	}
	se.CurOp = tmp
	isLeader := false
	Term := 0
	discard := -1
	for i2 := range se.CurOp {
		if se.CurOp[i2].Seq == se.SendSeq+1 {
			_, Term, isLeader = rf.Start(se.CurOp[i2])
			if !isLeader {
				se.reset(Term)
				discard = -1
				break
			}
			if Term > se.Term {
				se.reset(Term)
				discard = -1
				break
			}
			se.SendSeq += 1
			discard = i2
		}
	}
	if discard != -1 {
		se.CurOp = se.CurOp[discard+1:]
	}
	req := rcv + 1
	res := make([]ClientResult, 0)
	for i3 := range se.Res {
		if se.Res[i3].Seq < req {

		} else {
			res = se.Res[i3:]
			break
		}
	}
	//ackSeq:=max(sendSeq,applySeq)+1
	if se.SendSeq > se.ApplySeq {
		se.AckSeq = se.SendSeq + 1
	} else {
		se.AckSeq = se.ApplySeq + 1
		se.SendSeq = se.ApplySeq
	}
	//discard all the entry before sendSeq
	discard = -1
	for i2 := range se.CurOp {
		if se.CurOp[i2].Seq <= se.SendSeq {
			discard = i2
		} else {
			break
		}
	}
	if discard != -1 {
		se.CurOp = se.CurOp[discard+1:]
	}
	return res, se.AckSeq
}
func (se *session) AppendOp(mu *sync.Mutex, rf *raft.Raft, args *AppendOpArgs, reply *AppendOpReply) {
	mu.Lock()
	defer mu.Unlock()
	//DPrintf("AppendOp")
	if _, isLeader := rf.GetState(); isLeader {
		reply.NotLeader = false
	} else {
		reply.NotLeader = true
	}
	reply.Results, reply.Ack = se.putOp(rf, args.Ops, args.Rcv)
	reply.LastResultIndex = se.ApplySeq
}

type ClientOp struct {
	Seq int
	Key string
	Val string
	Op  string
}
type ClientResult struct {
	Seq int
	Err Err
	Val string
}
type AppendOpArgs struct {
	Uid uid
	Rcv int
	Ops []ClientOp
}
type AppendOpReply struct {
	Ack             int
	NotLeader       bool
	LastResultIndex int
	Results         []ClientResult
}
