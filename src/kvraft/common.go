package kvraft

import "sync"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

//const MAX_WINSZ = 20
const MAX_RETRY = 8

type Sequential struct {
	seq int
	mu  sync.Mutex
}

func (s *Sequential) GetSeq() int {
	s.mu.Lock()
	n := s.seq
	s.seq++
	s.mu.Unlock()
	return n
	// You will have to modify this function.
}

type ClientReply struct {
	Err   Err
	Value string
}
type ClientApplyMsg struct {
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	retChan chan ClientReply
}
type uid string

func randuid(n int) uid {
	return uid(randstring(n))
}
