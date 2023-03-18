package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		filename, programType, taskID, nReduce, nMap, pleaseWait, pleaseExit := CallGetTask()
		if pleaseExit {
			fmt.Println("Exiting.")
			os.Exit(1)
		}

		if pleaseWait {
			fmt.Println("Waiting.")
			time.Sleep(time.Second * 1)
			continue
		}

		if programType == Map {
			fmt.Printf("Received task: ProgramType=%v TaskID=%v Filename=%v\n", programType, taskID, filename)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				os.Exit(1)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
				os.Exit(1)
			}
			file.Close()
			intermediate := mapFunc(filename, string(content))

			outputFiles := make([]*os.File, nReduce)
			for i := range outputFiles {
				outputFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
				outputFile, _ := os.Create(outputFileName)
				outputFiles[i] = outputFile
			}

			outputKV := make([][]KeyValue, nReduce)
			for _, kv := range intermediate {
				reduceTaskID := ihash(kv.Key) % nReduce
				outputKV[reduceTaskID] = append(outputKV[reduceTaskID], kv)
			}

			for i := range outputKV {
				enc := json.NewEncoder(outputFiles[i])
				for _, kv := range outputKV {
					enc.Encode(kv)
				}
				outputFiles[i].Close()
			}
			CallCompletedTask(taskID, programType)
		} else {
			fmt.Printf("Received task: ProgramType=%v TaskID=%v\n", programType, taskID)
			KVs := make([]KeyValue, 0)

			for i := 0; i < nMap; i++ {
				filename := fmt.Sprintf("mr-%v-%v", i, taskID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
					os.Exit(1)
				}
				dec := json.NewDecoder(file)
				for {
					var kvs []KeyValue
					if err := dec.Decode(&kvs); err != nil {
						if err != io.EOF {
							log.Fatal(err)
						}
						break
					}
					//fmt.Println(kvs)
					KVs = append(KVs, kvs...)
				}
				file.Close()
			}

			outputFileName := fmt.Sprintf("mr-out-%d", taskID)
			outputFile, _ := os.Create(outputFileName)
			sort.Sort(ByKey(KVs))
			i := 0
			for i < len(KVs) {
				j := i + 1
				for j < len(KVs) && KVs[j].Key == KVs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, KVs[k].Value)
				}
				output := reduceFunc(KVs[i].Key, values)
				fmt.Fprintf(outputFile, "%v %v\n", KVs[i].Key, output)
				i = j
			}
			CallCompletedTask(taskID, programType)
		}
	}
}

func CallGetTask() (filename string, programType ProgramType,
	taskID int, nReduce int, nMap int, pleaseWait bool, pleaseExit bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
		os.Exit(1)
	}
	return reply.File, reply.ProgramType, reply.TaskID, reply.NumReduce, reply.NumMap, reply.PleaseWait, reply.PleaseExit
}

func CallCompletedTask(taskID int, programType ProgramType) {
	args := CompletedTaskArgs{}
	args.TaskID = taskID
	args.ProgramType = programType
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
