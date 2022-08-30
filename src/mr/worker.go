package mr

import (
	"6.824/util"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

const TempDir = "tmp"

// use ihash(key) % NReduce to choose the reduced
// AssignTask number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

type mapFunction func(string, string) []util.KeyValue
type reduceFunction func(string, []string) string

func atomicRename(oldName, newName string) {
	err := os.Rename(oldName, newName)
	util.CheckError(err, "Atomic Rename for %s to %s failed\n", oldName, newName)
}

// Process Functions
func processReduce(task Task, reducef reduceFunction) {
	// fetch reduce files
	reduceFiles, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", task.Id))
	util.CheckError(err, "Failed to list reduce files for AssignTask-%d", task.Id)

	log.Printf("Reducing Task %d\n", task.Id)

	// process reduce files
	buffer := make(map[string][]string)
	for _, fpath := range reduceFiles {
		file, err := os.Open(fpath)
		util.CheckError(err, "File opening %s failed\n", fpath)
		log.Printf("Opened file %s\n", fpath)
		dec := json.NewDecoder(file)
		for {
			var kv util.KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			buffer[kv.Key] = append(buffer[kv.Key], kv.Value)
		}
		file.Close()
	}

	// Put buffer results in file
	file, err := os.CreateTemp(TempDir, "reduce")
	if err != nil {
		log.Printf("File creation for reduce AssignTask %d failed", task.Id)
		return
	}
	for key, value := range buffer {
		_, err := fmt.Fprintf(file, "%v %v\n", key, reducef(key, value))
		util.CheckError(err, "Writing Final Reduce resuls for %s failed\n", file.Name())
	}

	atomicRename(file.Name(), fmt.Sprintf("mr-out-%d", task.Id))

	err = file.Close()
	util.CheckError(err, "File closing failed for %s", file.Name())
}

func processMap(task Task, nReduce int, mapf mapFunction) {
	file, err := os.Open(task.Filename)
	util.CheckError(err, "Cannot open %v\n", task.Filename)
	content, err := io.ReadAll(file)
	util.CheckError(err, "Cannot read %v\n", task.Filename)
	file.Close()
	log.Printf("Mapping AssignTask - %d : %s\n", task.Id, task.Filename)

	kva := mapf(task.Filename, string(content))
	reducePartition := make([][]util.KeyValue, nReduce)
	for _, kv := range kva {
		p := ihash(kv.Key) % nReduce
		reducePartition[p] = append(reducePartition[p], kv)
	}

	for i := 0; i < nReduce; i++ {
		tmpFile, err := os.CreateTemp(TempDir, "map")
		util.CheckError(err, "File creation for %d-%d failed\n", task.Id, i)
		enc := json.NewEncoder(tmpFile)
		for _, kv := range reducePartition[i] {
			_ = enc.Encode(&kv)
		}

		atomicRename(tmpFile.Name(),
			filepath.Dir(tmpFile.Name())+fmt.Sprintf("/mr-%d-%d", task.Id, i))
	}

	log.Println("Mapping Complete")

}

// Worker main/mrworker.go calls this function.
func Worker(mapf mapFunction, reducef reduceFunction) {
	err := os.Mkdir("tmp", 0777)
	util.CheckError(err, "Failed to create tmp dir")
	workerId, nReduce, ok := registerWorker()
	if !ok {
		log.Println("Worker Register Failed.Exiting")
		return
	} else {
		log.Printf("Worker #%d Online\n", workerId)
	}

	for {
		task, startTime, success := requestTask(workerId)
		if !success {
			log.Println("Task Request Failed.Exiting")
			return
		}
		if task.Type == ExitTask {
			return
		} else if task.Type == NoTask {
			continue
		} else if task.Type == MapTask {
			fmt.Println(task.Id, task.Type, task.Filename)
			processMap(task, nReduce, mapf)
		} else if task.Type == ReduceTask {
			processReduce(task, reducef)
		}
		reportTaskCompletion(workerId, startTime)
		time.Sleep(2 * time.Second)
	}
}

// RPC Calls

func registerWorker() (int, int, bool) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	success := call("Coordinator.RegisterWorker", &args, &reply)
	return reply.WorkerId, reply.NReduce, success
}

func requestTask(workerId int) (Task, int64, bool) {
	args := RequestTaskArgs{workerId}
	reply := RequestTaskReply{}
	success := call("Coordinator.RequestTask", &args, &reply)
	return reply.AssignTask, reply.StartTime, success
}

func reportTaskCompletion(id int, startTime int64) {
	args := ReportCompletionArgs{id, startTime}
	reply := ReportCompletionReply{}
	call("Coordinator.ReportCompletion", &args, &reply)
	if reply.Success {
		log.Println("Task Registered with Coordinator Successfully")
	} else {
		log.Println("Task Registration with Coordinator Failed")
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

	log.Println(err)
	return false
}
