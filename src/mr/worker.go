package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// for true {
		ok := call("Coordinator.Coordinate", oldTask, newTask)
		if ok {
			fmt.Printf("Handed in an old task: %v\n", oldTask)
			fmt.Printf("Accepted a new task: %v\n", *newTask)
		} else {
			fmt.Printf("call failed!\n")
		}

		if newTask != nil {
			if newTask.TaskType == 1 {
				// NOTE: map
				intermediate, err := workerMap(mapf, newTask.FName)
				if err != nil {
					// ERRO:
					log.Fatal(err)
				}
				// TODO: mr-%d-0
				filename := fmt.Sprintf("mr-%d-%d", newTask.TaskNum, 0)
				file, ok := os.Create(filename)
				if ok != nil {
					log.Fatalf("cannot create %v", filename)
				}
				for i := 0; i < len(intermediate); i++ {
					toWrite := fmt.Sprintf("%v %v\n", intermediate[i].Key, intermediate[i].Value)
					file.WriteString(toWrite)
				}
				file.Close()
			} else {
				// TODO: reduce
			}
		}
	// }

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// workerMap把filename和对应文件的内容传入mapf函数,并返回mapf函数运行的返回值
// 这提供了一层封装,让workerMap只执行函数,但是不需要处理RPC
func workerMap(mapf func(string, string) []KeyValue, filename string) ([]KeyValue, error) {
	if mapf == nil {
		// NOTE: []KeyValue可以为nil吗?
		return nil, fmt.Errorf("workerMap: mapf is nil!");
	}
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// TODO: 把kva写入mr-X-Y, where X is the Map task number, and Y is the reduce task number.
	return intermediate, nil
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
	// TODO: delete next line
	// fmt.Println(sockname)
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
