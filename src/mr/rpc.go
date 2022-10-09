package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// RPC包含一个文件指针
// Task和WorkerState的有用字段应该exported
type Task struct {
	TaskNum int
	TaskType int // map: 1, reduce: 2
	FName string
	isDone bool
}

// WorkerState表示worker的状态,应该当作RPC调用的参数,coordinator根据WorkerState来安排tasks
type WorkerState struct {
	IsIdle bool 
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
