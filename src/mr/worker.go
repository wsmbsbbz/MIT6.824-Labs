package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type syncWriter struct {
	mu sync.Mutex
	writer io.Writer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.writer.Write(p)
	if err != nil {
		log.Printf("*syncWriter.Write: %v\n", err)
	}
	// return w.Write(p)
	return n, err
}

var syncWriterMap sync.Map

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
	wg := sync.WaitGroup{}

	// for newTask.TaskType != TaskAllDone {
	for call("Coordinator.Coordinate", oldTask, newTask) {
		if newTask.TaskType == TaskMap {
			// wg.Add(1)
			// go func(newTask *Task) {
			// 	defer wg.Done()
				workerMap(mapf, newTask)
			// }(newTask)
		} else if newTask.TaskType == TaskReduce {
			wg.Add(1)
			go func(newTask *Task) {
				defer wg.Done()
				workerReduce(reducef, newTask)
			}(newTask)
		}
		oldTask, newTask = *newTask, &Task{}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	wg.Wait()
}

func workerMap(mapf func(string, string) []KeyValue, mTask *Task) error {
	if mapf == nil {
		// NOTE: []KeyValue可以为nil吗?
		return fmt.Errorf("workerMap: mapf is nil!");
	}
	intermediate := []KeyValue{}

	// key: tmpFilename, val: filename
	files := map[string]string{}
	encs := []*json.Encoder{}
	for i := 0; i < mTask.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mTask.TaskNum, i)
		// TODO: enable TempFile to workerReduce
		file, err := ioutil.TempFile(".", "*")
		defer file.Close()
		if err != nil {
			log.Printf("workerMap: %v, cannot create %v", err, filename)
		}
		files[file.Name()] = filename
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
	// atomically rename temp files to finished files
	for tmpFilename, filename := range files {
		os.Rename(tmpFilename, filename)
	}
	return nil
}

func workerReduce(reducef func(string, []string) string, rTask *Task) {
	kva := []KeyValue{}
	for i := 0; i < rTask.NMap; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, rTask.TaskNum))
		// log.Printf("workerReduce: opened mr-%d-%d\n", rTask.TaskNum, i)
		if err != nil {
			// ERRO: in job count test: FAIL
			// 2022/10/11 18:13:53 workerReduce: open mr-7-0: no such file or directory
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
	filename := fmt.Sprintf("mr-out-%d", rTask.TaskNum)
	// ofile, err := os.Create(filename)
	ofile, err := ioutil.TempFile(".", "*")
	defer ofile.Close()
	if err != nil {
		log.Printf("workerReduce: %v", err)
	}
	actual, _ := syncWriterMap.LoadOrStore(filename, &syncWriter{writer: ofile})
	sw := actual.(*syncWriter)

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
		// TODO: use syncWriter to write atomically
		fmt.Fprintf(sw, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), filename)
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
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
