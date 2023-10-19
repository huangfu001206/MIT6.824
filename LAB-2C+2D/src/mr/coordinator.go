package mr

import (
	//"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

/*
*

	任务 数据结构
*/
type chanTask struct {
	filename []string
	taskId   int
	taskType TaskTypes
}

//TaskType --- reply
/**
rpc通信用来标识是Map任务还是Reduce任务或其他等待信息
*/
type TaskTypes int

const (
	MapTask TaskTypes = iota + 1
	ReduceTask
	Wait
)

//MsgType --- args
/**
用来标识worker节点返回的信息为‘请求任务’还是‘报告任务已完成’
*/
type MsgType int

const (
	TaskApply MsgType = iota + 1
	TaskFinish
)

type Coordinator struct {
	// Your definitions here.
	nReduce         int
	nMapTask        int
	nReduceTask     int
	chanMap         chan chanTask
	chanReduce      chan chanTask
	mapTaskState    map[int]bool //正在进行中的map任务
	reduceTaskState map[int]bool //正在进行中的reduce人物
	workerIdMutex   sync.Mutex
	taskStateMutex  sync.Mutex
	taskCacheMutex  sync.Mutex
	mapJobDone      atomic.Bool      //map 任务是否已完成
	reduceJobDone   atomic.Bool      //reduce 任务是否已完成
	reduceTaskCache map[int][]string //用来暂存map产生的file名称（路径）
}

var mapWorkerIdInit atomic.Int32
var reduceWorkerIdInit atomic.Int32
var initTaskId atomic.Int32

func (c *Coordinator) runTaskApply(args *ApplyTaskArgs, reply *ApplyTaskReply) {
	if !c.mapJobDone.Load() {
		select {
		case task := <-c.chanMap:
			reply.TaskId = task.taskId
			reply.TaskType = MapTask
			reply.FileNameList = append(reply.FileNameList, task.filename[0])
			reply.WorkerId = int(mapWorkerIdInit.Load())
			reply.NReduce = c.nReduce
			mapWorkerIdInit.Add(1)
			go c.checkTimeOut(&task)
		default:
			reply.TaskType = Wait
			reply.WorkerId = 1
		}
	} else if !c.reduceJobDone.Load() {
		select {
		case task := <-c.chanReduce:
			reply.TaskId = task.taskId
			reply.TaskType = ReduceTask
			reply.FileNameList = task.filename
			reply.WorkerId = int(reduceWorkerIdInit.Load())
			reduceWorkerIdInit.Add(1)
			go c.checkTimeOut(&task)
		default:
			reply.TaskType = Wait
			reply.WorkerId = 1
		}
		return
	}
}

func (c *Coordinator) checkTimeOut(task *chanTask) {
	time.Sleep(10 * time.Second)
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()
	switch task.taskType {
	case MapTask:
		if !c.mapTaskState[task.taskId] {
			c.chanMap <- *task
		}
	case ReduceTask:
		if !c.reduceTaskState[task.taskId] {
			c.chanReduce <- *task
		}
	}
}

func getFilePath4ReduceId(filePath string) int {
	startIndex := strings.LastIndex(filePath, "-")
	num, _ := strconv.Atoi(filePath[startIndex+1:])
	return num
}

func addFilePath2TaskCache(filePath []string, c *Coordinator) {
	for _, filePath := range filePath {
		rTaskId := getFilePath4ReduceId(filePath)
		c.reduceTaskCache[rTaskId] = append(c.reduceTaskCache[rTaskId], filePath)
	}
}
func (c *Coordinator) runTaskFinish(args *ApplyTaskArgs, reply *ApplyTaskReply) {
	switch args.TaskType {
	case MapTask:
		//fmt.Println("runTaskFinish MapTask: ", args)
		c.taskCacheMutex.Lock()
		addFilePath2TaskCache(args.FilePath, c)
		c.taskCacheMutex.Unlock()

		flag := false
		c.taskStateMutex.Lock()
		if !c.mapTaskState[args.TaskId] {
			c.nMapTask--
			if c.nMapTask <= 0 {
				c.putTaskToReduce()
				flag = true
			}
		}
		c.mapTaskState[args.TaskId] = true
		c.taskStateMutex.Unlock()
		c.mapJobDone.Store(flag)
		//fmt.Println("MapTaskComplete: ", flag)
	case ReduceTask:
		//fmt.Println("runTaskFinish ReduceTask: ", args)
		c.taskStateMutex.Lock()
		flag := false
		if !c.reduceTaskState[args.TaskId] {
			c.nReduceTask--
			if c.nReduceTask <= 0 {
				flag = true
			}
		}
		c.reduceTaskState[args.TaskId] = true
		c.taskStateMutex.Unlock()
		c.reduceJobDone.Store(flag)
		//fmt.Println("ReduceTaskComplete: ", flag)
	}
}

func (c *Coordinator) RunTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	switch args.MessageType {
	case TaskApply:
		c.runTaskApply(args, reply)
	case TaskFinish:
		c.runTaskFinish(args, reply)
	}
	return nil
}

func (c *Coordinator) putTaskToMap(files []string) {
	//fmt.Println("************** putTaskToMap **************")
	for index, f := range files {
		task := chanTask{
			taskId:   index,
			filename: []string{f},
			taskType: MapTask,
		}
		c.chanMap <- task
	}
}
func (c *Coordinator) putTaskToReduce() {
	//fmt.Println("************** putTaskToReduce **************")
	nReduce := int(c.nReduce)
	for i := 0; i < nReduce; i++ {
		task := chanTask{
			filename: c.reduceTaskCache[i],
			taskId:   i,
			taskType: ReduceTask,
		}
		c.chanReduce <- task
	}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	return c.reduceJobDone.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Println("*********** coordinator start **************")
	c := Coordinator{}
	//fmt.Println(len(files))
	// Your code here.
	c.nReduce = nReduce
	c.nMapTask = len(files)
	c.nReduceTask = c.nReduce
	c.chanMap = make(chan chanTask, len(files))
	c.chanReduce = make(chan chanTask, nReduce)
	c.mapTaskState = make(map[int]bool)
	c.reduceTaskState = make(map[int]bool)
	c.reduceTaskCache = make(map[int][]string)
	c.mapJobDone.Store(false)
	c.reduceJobDone.Store(false)
	mapWorkerIdInit.Store(1000)
	reduceWorkerIdInit.Store(100)
	initTaskId.Store(0)
	//map 任务放入 chan
	go c.putTaskToMap(files)

	c.server()
	return &c
}
