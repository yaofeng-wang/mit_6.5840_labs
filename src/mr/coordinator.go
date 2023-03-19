package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu sync.Mutex

	files []string

	nMap                 int
	assignedMapTasks     []bool
	completedMapTasks    []bool
	numCompletedMapTasks int

	nReduce                 int
	assignedReduceTasks     []bool
	completedReduceTasks    []bool
	numCompletedReduceTasks int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.numCompletedMapTasks < len(c.files) {
		for i := range c.assignedMapTasks {
			if !c.assignedMapTasks[i] {
				reply.File = c.files[i]
				reply.ProgramType = Map
				reply.TaskID = i
				reply.NumReduce = c.nReduce
				c.assignedMapTasks[i] = true
				fmt.Printf("[GetTask]: ProgramType=%v TaskID=%v File=%v\n",
					reply.ProgramType, reply.TaskID, reply.File)
				go func(taskID int) {
					time.Sleep(time.Second * 10)
					c.mu.Lock()
					defer c.mu.Unlock()
					if !c.completedMapTasks[taskID] {
						c.assignedMapTasks[taskID] = false
					}
				}(i)
				return nil
			}
		}
		fmt.Println("[GetTask]: Please Wait For Map Tasks")
		reply.PleaseWait = true
		return nil
	}

	if c.numCompletedReduceTasks < c.nReduce {
		for i := range c.assignedReduceTasks {
			if !c.assignedReduceTasks[i] {
				reply.ProgramType = Reduce
				reply.TaskID = i
				reply.NumMap = len(c.files)
				c.assignedReduceTasks[i] = true
				fmt.Printf("[GetTask]: ProgramType=%v TaskID=%v\n", reply.ProgramType, reply.TaskID)
				go func(taskID int) {
					time.Sleep(time.Second * 10)
					c.mu.Lock()
					defer c.mu.Unlock()
					if !c.assignedReduceTasks[taskID] {
						c.assignedReduceTasks[taskID] = false
					}
				}(i)
				return nil
			}
		}
		fmt.Println("[GetTask]: Please Wait For Reduce Tasks")
		reply.PleaseWait = true
		return nil
	}

	fmt.Println("[GetTask]: Please Exit")
	reply.PleaseExit = true

	return nil
}

func (c *Coordinator) CompletedTask(args *CompletedTaskArgs, reply *CompletedTaskReply) error {
	taskID, programType := args.TaskID, args.ProgramType
	fmt.Printf("[CompletedTask]: ProgramType=%v TaskID=%v\n", programType, taskID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if programType == Map {
		if !c.completedMapTasks[taskID] {
			c.numCompletedMapTasks++
		}
		c.completedMapTasks[taskID] = true
	} else {
		if !c.completedReduceTasks[taskID] {
			c.numCompletedReduceTasks++
		}
		c.completedReduceTasks[taskID] = true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	err = os.Remove(sockname)
	if err != nil {
		log.Fatal(err)
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done is called by main/mrcoordinator.go periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.numCompletedMapTasks == c.nMap && c.numCompletedReduceTasks == c.nReduce {
		fmt.Println("All done.")
		return true
	}
	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files

	c.nMap = len(files)
	c.assignedMapTasks = make([]bool, c.nMap)
	c.completedMapTasks = make([]bool, c.nMap)

	c.nReduce = nReduce
	c.assignedReduceTasks = make([]bool, c.nReduce)
	c.completedReduceTasks = make([]bool, c.nReduce)

	c.server()
	return &c
}
