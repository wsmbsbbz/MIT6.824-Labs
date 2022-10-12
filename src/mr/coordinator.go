package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	// Coordinator.state
	CoorMapping = 1
	CoorReducing = 2
	CoorAllDone = 3

	// Task.TaskType
	TaskMap = 1
	TaskReduce = 2
	TaskAllDone = 3

	// mapTask/reduceTask state
	TaskWating = 0
	TaskRunning = 1
	TaskDone = 2
)


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	state int // mapping: 1, reducing: 2, allDone: 3

	nMap int
	mTasks []mapTask
	mapDoneCount int

	nReduce int
	rTasks []reduceTask
	reduceDoneCount int

}

type mapTask struct {
	fName string
	state int
	beginTime time.Time
}

type reduceTask struct {
	taskNum int
	state int
	beginTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
// args可以是一个表示worker的状态,
// oldTask, newTask
func (c *Coordinator) Coordinate(oldTask Task, newTask *Task) error {
	log.Printf("c.Coordinate: oldTask: %v\n", oldTask)
	// log.Printf("c.Coordinate: newTask: %v\n", newTask)
	c.coordinateOldTask(oldTask)
	c.mu.Lock()
	if c.mapDoneCount > len(c.mTasks) {
		log.Printf("c.Coordinate: unvalid c.mapDoneCount %v with len(c.files) %v\n",
			c.mapDoneCount, len(c.mTasks))
	}
	c.mu.Unlock()

	// NOTE: 分配newTask
	c.coordinateNewTask(newTask)

	// TODO: 目前是分配一个FName,需要改为为每个worker分配一个不同的FName
	return nil
}

func (c *Coordinator) coordinateOldTask(oldTask Task)  {
	// 无oldTask,不处理,说明这个worker是第一次申请task
	if oldTask.TaskType == 0 {
		return
	}
	// 获取,释放 c.filesLock
	c.mu.Lock()
	defer c.mu.Unlock()

	if oldTask.TaskNum < 0 {
		log.Printf("coordinateOldTask: unvalid TaskNum %v\n", oldTask.TaskNum)
	}

	if oldTask.TaskType == TaskMap {
		if c.mTasks[oldTask.TaskNum].state != TaskDone {
			c.mTasks[oldTask.TaskNum].state = TaskDone
			c.mapDoneCount++
		}
		if c.mapDoneCount == len(c.mTasks) {
			c.state = CoorReducing
		}
	} else if oldTask.TaskType == TaskReduce {
		if c.rTasks[oldTask.TaskNum].state != TaskDone {
			c.rTasks[oldTask.TaskNum].state = TaskDone
			c.reduceDoneCount++
		}
		if c.reduceDoneCount == c.nReduce {
			c.state = CoorAllDone
		}
	} else if oldTask.TaskType == TaskAllDone {
		// log.Printf("coordinateOldTask: TaskAllDone %v\n", oldTask.TaskType)
	} else {
		log.Fatalf("coordinateOldTask: unvalid TaskType %v\n", oldTask.TaskType)
	}
}

func (c *Coordinator) coordinateNewTask(newTask *Task)  {
	c.mu.Lock()
	defer c.mu.Unlock()
	// log.Printf("coordinateNewTask: c.state %v", c.state)

	if c.state == CoorMapping {
		for i := 0; i < len(c.mTasks); i++ {
			if c.mTasks[i].state == TaskWating || ( c.mTasks[i].state == TaskRunning && time.Since(c.mTasks[i].beginTime) >= time.Second * 10) {
				if c.mTasks[i].state == TaskRunning && time.Since(c.mTasks[i].beginTime) >= time.Second * 10 {
					log.Printf("mTask time expired")
				}
				c.mTasks[i].state = TaskRunning
				newTask.TaskNum = i
				newTask.TaskType = TaskMap
				newTask.FName = c.mTasks[i].fName
				newTask.NMap = c.nMap
				newTask.NReduce = c.nReduce
				break
			}
		}
	} else if c.state == CoorReducing {
		for i := 0; i < len(c.rTasks); i++ {
			if c.rTasks[i].state == TaskWating || ( c.rTasks[i].state == TaskRunning && time.Since(c.rTasks[i].beginTime) >= time.Second * 10) {
				if c.rTasks[i].state == TaskRunning && time.Since(c.rTasks[i].beginTime) >= time.Second * 10 {
					log.Printf("rTask time expired")
				}
				c.rTasks[i].state = TaskRunning
				newTask.TaskNum = i
				newTask.TaskType = TaskReduce
				newTask.FName = ""
				newTask.NMap = c.nMap
				newTask.NReduce = c.nReduce
				break
			}
		}
	} else if c.state == CoorAllDone {
		newTask.TaskType = TaskAllDone
	} else {
		log.Fatalf("coordinateNewTask: unvalid c.state %v\n", c.state)
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state == CoorAllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := &Coordinator{state: CoorMapping, nMap: len(files), nReduce: nReduce}
	for _, filename := range files{
		c.mTasks = append(c.mTasks, mapTask{filename, TaskWating, time.Now()})
	}
	for i := 0; i < nReduce; i++ {
		c.rTasks = append(c.rTasks, reduceTask{i, TaskWating, time.Now()})
	}
	// log.Printf("MakeCoordinator: %v\n", c)


	c.server()
	return c
}
