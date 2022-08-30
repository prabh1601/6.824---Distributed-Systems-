package mr

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	NReduce  int
	WorkerId int
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	AssignTask Task
	StartTime  int64
}

type ReportCompletionArgs struct {
	WorkerId  int
	StartTime int64
}

type ReportCompletionReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
