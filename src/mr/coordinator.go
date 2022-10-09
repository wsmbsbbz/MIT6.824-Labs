package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

const (
	// Coordinator.state
	MAPPING = 1
	REDUCING = 2

	// Task.TaskType
	MAP = 1
	REDUCE = 2
)


type Coordinator struct {
	// Your definitions here.
	files []MRFile
	// TODO: 处理多个文件
	filename string
	// TODO: 拆分为NReduce个reduce文件
	NReduce int
	// map和reduce都完成之后,isDone的值才应该为true
	isDone bool
	state int // mapping: 1, reducing: 2
}

type MRFile struct {
	FName string
	isDone bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// args可以是一个表示worker的状态,
// oldTask, newTask
func (c *Coordinator) Coordinate(oldTask *Task, newTask *Task) error {
	// TODO: 处理并发
	log.Printf("New RPC call: %v\n", newTask)
	log.Printf("oldTask: %v\n", oldTask)
	// TODO: 处理oldTask
	if oldTask.TaskNum == -1 {
		
	}
	newTask.FName = c.filename
	newTask.TaskType = 1
	log.Println(newTask)

	// TODO: 分配newTask
	// TODO: 目前是分配一个FName,需要改为为每个worker分配一个不同的FName
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.


	return c.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{filename: files[0]}

	// Your code here.


	c.server()
	return &c
}
