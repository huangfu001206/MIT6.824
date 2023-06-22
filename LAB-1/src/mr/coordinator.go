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
map任务 数据结构
*/
type chanMapTask struct {
	filename string
	taskId   int
}

/*
*
reduce任务 数据结构
*/
type chanReduceTask struct {
	filename []string
	taskId   int
}

// TaskState
/**
用来标识任务的开始时间、任务id
*/
type TaskState struct {
	startTime time.Time
	workId    int
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
	nReduce              atomic.Int32
	nTask                atomic.Int32
	chanMap              chan chanMapTask
	chanReduce           chan chanReduceTask
	mapTaskState         map[int]TaskState //正在进行中的map任务
	reduceTaskState      map[int]TaskState //正在进行中的reduce人物
	mTaskId4FileName     map[int]string    //map任务中taskId与filename的对应关系
	rTaskId4FileNameList map[int][]string  //reduce任务中taskId与filenameList的对应关系
	workerIdMutex        sync.Mutex
	taskStateMutex       sync.Mutex
	id4FileNameMutex     sync.Mutex
	mapJobDone           atomic.Bool      //map 任务是否已完成
	reduceJobDone        atomic.Bool      //reduce 任务是否已完成
	reduceTaskCache      map[int][]string //用来暂存map产生的file名称（路径）
}

var mapWorkerIdInit atomic.Int32
var reduceWorkerIdInit atomic.Int32
var initTaskId atomic.Int32

func runTaskApply(args *ApplyTaskArgs, reply *ApplyTaskReply, c *Coordinator) {
	if !c.mapJobDone.Load() {
		select {
		case task := <-c.chanMap:
			reply.TaskId = task.taskId
			reply.TaskType = MapTask
			reply.FileNameList = append(reply.FileNameList, task.filename)
			reply.WorkerId = int(mapWorkerIdInit.Load())
			reply.nReduce = int(c.nReduce.Load())
			mapWorkerIdInit.Add(1)
			c.taskStateMutex.Lock()
			c.mapTaskState[task.taskId] = TaskState{
				startTime: time.Now(),
				workId:    reply.WorkerId,
			}
			c.taskStateMutex.Unlock()
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
			c.reduceTaskState[task.taskId] = TaskState{
				startTime: time.Now(),
				workId:    reply.WorkerId,
			}
			c.id4FileNameMutex.Lock()
			c.rTaskId4FileNameList[task.taskId] = task.filename
			c.id4FileNameMutex.Unlock()
		default:
			reply.TaskType = Wait
			reply.WorkerId = 1
		}
		return
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
func runTaskFinish(args *ApplyTaskArgs, reply *ApplyTaskReply, c *Coordinator) {
	switch args.TaskType {
	case MapTask:
		addFilePath2TaskCache(args.FilePath, c)

		c.taskStateMutex.Lock()
		delete(c.mapTaskState, args.TaskId)
		c.taskStateMutex.Unlock()

		c.nTask.Add(-1)
		if c.nTask.Load() <= 0 {
			c.putTaskToReduce()
			c.mapJobDone.Store(true)
		}
	case ReduceTask:
		c.taskStateMutex.Lock()
		delete(c.reduceTaskState, args.TaskId)
		c.taskStateMutex.Unlock()

		c.nReduce.Add(-1)
		if c.nReduce.Load() <= 0 {
			c.reduceJobDone.Store(true)
		}
	}
}

func (c *Coordinator) CheckTaskTimeOut() {
	for {
		c.taskStateMutex.Lock()
		if !c.mapJobDone.Load() {
			for k, v := range c.mapTaskState {
				now := time.Now()
				if now.Sub(v.startTime) >= time.Second*10 {
					task := chanMapTask{
						filename: c.mTaskId4FileName[k],
						taskId:   k,
					}
					go func() {
						c.chanMap <- task
					}()
					delete(c.mapTaskState, k)
				}
			}
		} else if !c.reduceJobDone.Load() {
			for k, v := range c.reduceTaskState {
				now := time.Now()
				if now.Sub(v.startTime) >= time.Second*10 {
					task := chanReduceTask{
						filename: c.rTaskId4FileNameList[k],
						taskId:   k,
					}
					go func() {
						c.chanReduce <- task
					}()

					delete(c.reduceTaskState, k)
				}
			}
		}
		c.taskStateMutex.Unlock()
		time.Sleep(time.Duration(time.Second * 5))
	}
}

func (c *Coordinator) RunTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	switch args.MessageType {
	case TaskApply:
		runTaskApply(args, reply, c)
	case TaskFinish:
		runTaskFinish(args, reply, c)
	}
	return nil
}

func (c *Coordinator) putTaskToMap(files []string) {
	for index, f := range files {
		task := chanMapTask{
			taskId:   index,
			filename: f,
		}
		//fmt.Println(task.taskId, task.filename)
		c.chanMap <- task
		c.id4FileNameMutex.Lock()
		c.mTaskId4FileName[index] = f
		c.id4FileNameMutex.Unlock()
	}
}
func (c *Coordinator) putTaskToReduce() {
	nReduce := int(c.nReduce.Load())
	for i := 0; i < nReduce; i++ {
		task := chanReduceTask{
			filename: c.reduceTaskCache[i],
			taskId:   i,
		}
		c.chanReduce <- task
		c.rTaskId4FileNameList[i] = task.filename
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
	c.nReduce.Store(int32(nReduce))
	c.nTask.Store(int32(len(files)))
	c.chanMap = make(chan chanMapTask, len(files))
	c.chanReduce = make(chan chanReduceTask, nReduce)
	c.mapTaskState = make(map[int]TaskState)
	c.reduceTaskState = make(map[int]TaskState)
	c.mTaskId4FileName = make(map[int]string)
	c.rTaskId4FileNameList = make(map[int][]string)
	c.reduceTaskCache = make(map[int][]string)
	c.mapJobDone.Store(false)
	c.reduceJobDone.Store(false)
	mapWorkerIdInit.Store(1000)
	reduceWorkerIdInit.Store(100)
	initTaskId.Store(0)
	//map 任务放入 chan
	go c.putTaskToMap(files)
	//检查任务有没有在指定时间内完成
	go c.CheckTaskTimeOut()

	c.server()
	return &c
}
