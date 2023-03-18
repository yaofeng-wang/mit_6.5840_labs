package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex
	files     []string
	nReduce   int
	completed []bool
	assigned  []bool
	timeout   chan int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	hasAssignedTask := false
	for i := range c.assigned {
		if !c.assigned[i] {
			reply.File = c.files[i]
			reply.ProgramType = Map
			reply.TaskID = i
			c.assigned[i] = true
			hasAssignedTask = true
			fmt.Printf("[GetTask]: %v %v\n", reply.TaskID, reply.File)
			return nil
		}
	}
	if !hasAssignedTask {
		reply.PleaseExit = true
		fmt.Println("Please exit.")
	}
	return nil
}

type CompletedTaskArgs struct {
	TaskID int
}

type CompletedTaskReply struct {
}

func (c *Coordinator) CompletedTask(args *CompletedTaskArgs, reply *CompletedTaskReply) error {
	taskID := args.TaskID
	fmt.Printf("[CompletedTask]: %v\n", taskID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.completed[taskID] = true

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done is called by main/mrcoordinator.go periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for i := range c.completed {
		if !c.completed[i] {
			return false
		}
	}
	fmt.Println("All done.")
	return true
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.assigned = make([]bool, len(files))
	c.completed = make([]bool, len(files))
	c.timeout = make(chan int)

	c.server()
	return &c
}
