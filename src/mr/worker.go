package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main/mrworker.go
func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {
	for {

		filename, programType, taskID, pleaseExit := CallGetTask()
		if pleaseExit {
			fmt.Println("Exiting.")
			return
		}

		fmt.Printf("Received unfinished task: %v\n", taskID)
		if programType == Map {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
				return
			}
			file.Close()
			kva := mapFunc(filename, string(content))
			fmt.Println(filename, ":", kva)
			CallCompletedTask(taskID)
		}
	}
}

func CallGetTask() (filename string, programType ProgramType, taskID int, pleaseExit bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
		os.Exit(1)
	}
	return reply.File, reply.ProgramType, reply.TaskID, reply.PleaseExit
}

func CallCompletedTask(taskID int) {
	args := CompletedTaskArgs{}
	args.TaskID = taskID
	reply := CompletedTaskReply{}
	ok := call("Coordinator.CompletedTask", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
		os.Exit(1)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
