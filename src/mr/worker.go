package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	intermediateFileNameFormat = "mr-%v-%v"
	outputFileNameFormat       = "mr-out-%v"

	DEBUG = false
)

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
	_, err := h.Write([]byte(key))
	if err != nil {
		log.Fatal(err)
	}
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main/mrworker.go
func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {
	workerID := os.Getpid()
	fmt.Printf("%v Started\n", workerID)

	for {
		filename, programType, taskID, nReduce, nMap, pleaseWait, pleaseExit := CallGetTask()
		if pleaseExit {
			fmt.Printf("%v Exiting.\n", workerID)
			os.Exit(0)
		}

		if pleaseWait {
			fmt.Printf("%v Waiting.\n", workerID)
			time.Sleep(time.Second * 1)
			continue
		}

		if programType == Map {
			if DEBUG {
				fmt.Printf(
					"%v Received task: ProgramType=%v TaskID=%v Filename=%v\n",
					workerID, programType, taskID, filename,
				)
			}
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			err = file.Close()
			if err != nil {
				log.Fatal(err)
			}
			intermediate := mapFunc(filename, string(content))
			//fmt.Println(intermediate)
			outputFiles := make([]*os.File, nReduce)
			for i := range outputFiles {
				outputFileName := fmt.Sprintf(intermediateFileNameFormat, taskID, i)
				outputFile, err := os.CreateTemp("", outputFileName)
				if err != nil {
					log.Fatal(err)
				}
				outputFiles[i] = outputFile
			}

			outputKV := make([][]KeyValue, nReduce)
			for _, kv := range intermediate {
				reduceTaskID := ihash(kv.Key) % nReduce
				outputKV[reduceTaskID] = append(outputKV[reduceTaskID], kv)
			}

			//fmt.Println(outputKV)
			path, _ := os.Getwd()
			for i := range outputKV {
				enc := json.NewEncoder(outputFiles[i])
				err = enc.Encode(&outputKV[i])
				if err != nil {
					log.Fatal(err)
				}
				err = outputFiles[i].Close()
				if err != nil {
					log.Fatal(err)
				}
				err = os.Rename(outputFiles[i].Name(),
					filepath.Join(path, fmt.Sprintf(intermediateFileNameFormat, taskID, i)))
				if err != nil {
					log.Fatal(err)
				}
			}
			CallCompletedTask(taskID, programType)
		} else {
			if DEBUG {
				fmt.Printf(
					"%v Received task: ProgramType=%v TaskID=%v\n",
					workerID, programType, taskID,
				)
			}
			KVs := make([]KeyValue, 0)

			for i := 0; i < nMap; i++ {
				filename := fmt.Sprintf(intermediateFileNameFormat, i, taskID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
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
				err = file.Close()
				if err != nil {
					log.Fatal(err)
				}
			}

			outputFileName := fmt.Sprintf(outputFileNameFormat, taskID)
			outputFile, err := os.CreateTemp("", outputFileName)
			if err != nil {
				log.Fatal(err)
			}
			sort.Sort(ByKey(KVs))
			i := 0
			for i < len(KVs) {
				j := i + 1
				for j < len(KVs) && KVs[j].Key == KVs[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, KVs[k].Value)
				}
				output := reduceFunc(KVs[i].Key, values)
				_, err = fmt.Fprintf(outputFile, "%v %v\n", KVs[i].Key, output)
				if err != nil {
					log.Fatal(err)
				}
				i = j
			}
			path, err := os.Getwd()
			if err != nil {
				log.Fatal(err)
			}
			err = os.Rename(outputFile.Name(), filepath.Join(path, outputFileName))
			if err != nil {
				log.Fatal(err)
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
		log.Fatal("call failed!")
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
		log.Fatal("call failed!")
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

	log.Fatal(err)
	return false
}
