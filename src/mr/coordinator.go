package mr

import (
	"6.824/util"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

type Task struct {
	Type     TaskType
	Id       int
	Filename string
}

type Coordinator struct {
	// Your definitions here.
	ongoingTasks sync.Map
	pendingTasks chan Task
	mapCount     atomic.Int32
	reduceCount  atomic.Int32
	workerCount  atomic.Int32
	nReduce      int
}

const timeout int64 = 10 * 1000

type TaskStamp util.Pair[int, int64]

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	reply.WorkerId = int(c.workerCount.Add(1))
	reply.NReduce = c.nReduce
	log.Printf("Worker %d Registered\n", reply.WorkerId)
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if c.mapCount.Load() == 0 && c.reduceCount.Load() == 0 {
		reply.AssignTask = Task{ExitTask, -1, ""}
	} else if len(c.pendingTasks) != 0 {
		reply.AssignTask = <-c.pendingTasks
		reply.StartTime = time.Now().UnixMilli()
		c.ongoingTasks.Store(TaskStamp{args.WorkerId, reply.StartTime}, reply.AssignTask)
	} else {
		reply.AssignTask = Task{NoTask, -1, ""}
	}
	return nil
}

func (c *Coordinator) ReportCompletion(args *ReportCompletionArgs, reply *ReportCompletionReply) error {
	// This worker was not marked dead by heartbeat and hence it is actually the one that is completing this AssignTask
	task, del := c.ongoingTasks.LoadAndDelete(TaskStamp{args.WorkerId, args.StartTime})
	if !del {
		reply.Success = false
		return nil
	}

	if task.(Task).Type == MapTask {
		c.mapCount.Add(-1)

		if c.mapCount.Load() == 0 {
			for i := 0; i < c.nReduce; i++ {
				t := Task{ReduceTask, i, ""}
				c.pendingTasks <- t
			}
		}
	} else {
		c.reduceCount.Add(-1)
	}

	reply.Success = true
	return nil
}

func (c *Coordinator) heartbeat() {
	for {
		deadWorkers := make([]TaskStamp, 0)
		c.ongoingTasks.Range(func(stamp, task any) bool {
			currentTime := time.Now().UnixMilli()
			if currentTime-(stamp.(TaskStamp).Second) > timeout {
				deadWorkers = append(deadWorkers, stamp.(TaskStamp))
			}
			return true
		})

		for _, stamp := range deadWorkers {
			task, del := c.ongoingTasks.LoadAndDelete(stamp)
			if del {
				log.Printf("Rescheduling Task %d-%d Due to Timeout", task.(Task).Type, task.(Task).Id)
				if task.(Task).Type == MapTask {
					c.pendingTasks <- task.(Task)
				} else {
					c.pendingTasks <- task.(Task)
				}
			}
		}
		time.Sleep(time.Second * 2)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	registerErr := rpc.Register(c)
	if registerErr != nil {
		return
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Println("Coordinator Server Established")
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceCount.Load() == 0 && c.mapCount.Load() == 0
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		sync.Map{},
		make(chan Task, 20),
		atomic.Int32{},
		atomic.Int32{},
		atomic.Int32{},
		nReduce}
	c.mapCount.Store(int32(len(files)))
	c.reduceCount.Store(int32(nReduce))

	log.Println("Coordinator Object Created")

	for i, f := range files {
		t := Task{MapTask, i, f}
		c.pendingTasks <- t
	}

	log.Println("Map Tasks Scheduled")

	c.server()
	go c.heartbeat()
	return &c
}
