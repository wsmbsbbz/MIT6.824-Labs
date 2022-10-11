package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// type syncWriter struct {
// 	mu sync.Mutex
// 	writer io.Writer
// }

// func (w *syncWriter) Write(p []byte) (n int, err error) {
// 	w.mu.Lock()
// 	defer w.mu.Unlock()
// 	return w.Write(p)
// }

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
// 通过RPC调用接受一个文件名,对文件执行mapf或者reducef函数
// Worker应该
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// NOTE: ask for a work from coordinator
	oldTask := Task{}
	newTask := &Task{}

	// for newTask.TaskType != TaskAllDone {
	for true {
		ok := call("Coordinator.Coordinate", oldTask, newTask)
		if ok {
			// log.Printf("Handed in an old task: %v\n", oldTask)
			// log.Printf("Accepted a new task: %v\n", *newTask)
		} else {
			log.Printf("call failed!\n")
		}

		if newTask.TaskType == TaskAllDone || newTask.TaskType == 0 {
			break
		} else if newTask.TaskType == TaskMap {
			workerMap(mapf, newTask)
		} else if newTask.TaskType == TaskReduce {
			workerReduce(reducef, newTask)
		} else if newTask.TaskType == 0 {
			time.Sleep(time.Second)
		}
		oldTask, newTask = *newTask, &Task{}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func workerMap(mapf func(string, string) []KeyValue, mTask *Task) error {
	if mapf == nil {
		// NOTE: []KeyValue可以为nil吗?
		return fmt.Errorf("workerMap: mapf is nil!");
	}
	intermediate := []KeyValue{}

	files := []*os.File{}
	encs := []*json.Encoder{}
	for i := 0; i < mTask.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mTask.TaskNum, i)
		file, err := os.Create(filename)
		defer file.Close()
		if err != nil {
			log.Printf("workerMap: %v, cannot create %v", err, filename)
		}
		files = append(files, file)
		encs = append(encs, json.NewEncoder(file))
	}

	// read from input file
	file, err := os.Open(mTask.FName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", mTask.FName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mTask.FName)
	}
	kva := mapf(mTask.FName, string(content))
	intermediate = append(intermediate, kva...)

	// output to outputName file
	// for i := 0; i < len(intermediate); i++ {
	for _, kv := range intermediate {
		err := encs[ihash(kv.Key) % mTask.NReduce].Encode(&kv)
		if err != nil {
			log.Fatalf("enc.Encode\n")
		}
	}
	return nil
}

func workerReduce(reducef func(string, []string) string, rTask *Task) {
	// files := []*os.File{}
	kva := []KeyValue{}
	for i := 0; i < rTask.NMap; i++ {
		// TODO mr-X-Y
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, rTask.TaskNum))
		// log.Printf("workerReduce: opened mr-%d-%d\n", rTask.TaskNum, i)
		if err != nil {
			log.Fatalf("workerReduce: %v", err)
		}


		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	mrOut, err := os.Create(fmt.Sprintf("mr-out-%d", rTask.TaskNum))
	defer mrOut.Close()
	if err != nil {
		log.Printf("workerReduce: %v", err)
	}

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(mrOut, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
