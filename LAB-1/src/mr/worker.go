package mr

import (
	"encoding/json"
	"fmt"

	//"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func createMapTempFile(nReduce int, workerId int, filenameList *[]string, tempFileNameList *[]string) []*json.Encoder {
	var encList []*json.Encoder
	for i := 0; i < nReduce; i++ {
		var build strings.Builder
		build.WriteString("rm-")
		build.WriteString(strconv.Itoa(workerId))
		build.WriteString("-")
		build.WriteString(strconv.Itoa(i))
		filename := build.String()
		f, _ := ioutil.TempFile("./", filename)
		//f, _ := os.Create(filename)
		*filenameList = append(*filenameList, filename)
		*tempFileNameList = append(*tempFileNameList, f.Name())
		encList = append(encList, json.NewEncoder(f))
	}
	return encList
}
func runMapByWorker(reply *ApplyTaskReply, mapf func(string, string) []KeyValue) {
	//pathPrefix := "../main/"
	pathPrefix := ""
	reply.nReduce = 10
	fullPath := pathPrefix + reply.FileNameList[0]
	file, err := os.Open(fullPath)
	if err != nil {
		log.Fatalf("cannot open %v", fullPath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fullPath)
	}
	file.Close()
	kva := mapf(fullPath, string(content))
	var fileNameList []string
	var tempFileNameList []string
	encList := createMapTempFile(reply.nReduce, reply.WorkerId, &fileNameList, &tempFileNameList)
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.nReduce
		encList[index].Encode(kv)
	}
	reNameFileAndDeleteTempFile(&fileNameList, &tempFileNameList)
	WorkerFinishWork(reply.TaskType, reply.TaskId, fileNameList)
}
func reNameFileAndDeleteTempFile(fileNameList *[]string, tempFileNameList *[]string) {
	for index, _ := range *fileNameList {
		//_, err := os.Stat((*fileNameList)[index])
		//if err == nil {
		//	//如果文件存在
		//	err := os.Remove((*tempFileNameList)[index])
		//	if err != nil {
		//		log.Fatalf("file remove failed, err: %v", err)
		//	}
		//	continue
		//}
		err := os.Rename((*tempFileNameList)[index], (*fileNameList)[index])
		////fmt.Println((*tempFileNameList)[index], (*fileNameList)[index])
		if err != nil {
			log.Fatalf("file rename failed, err: %v", err)
		}
	}
}
func reduceReadFile(fileNameList []string) []KeyValue {
	filePathPrefix := ""
	kva := []KeyValue{}
	for _, filename := range fileNameList {
		fullPath := filePathPrefix + filename
		file, err := os.Open(fullPath)
		if err != nil {
			return kva
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
	return kva
}

func runReduceByWorker(reply *ApplyTaskReply, reducef func(string, []string) string) {
	kvList := reduceReadFile(reply.FileNameList)
	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, _ := ioutil.TempFile("./", oname)
	//ofile, _ := os.Create(oname)
	i := 0
	for i < len(kvList) {
		j := i + 1
		for j < len(kvList) && kvList[j].Key == kvList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvList[k].Value)
		}
		output := reducef(kvList[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvList[i].Key, output)
		i = j
	}
	ofile.Close()
	err := os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("file rename failed, err: %v", err)
	}
	WorkerFinishWork(reply.TaskType, reply.TaskId, reply.FileNameList)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		reply, ok := ApplyForTask()
		if ok == false {
			break
		}
		if reply.WorkerId != 0 {
			taskType := reply.TaskType
			switch taskType {
			case MapTask:
				runMapByWorker(&reply, mapf)
			case ReduceTask:
				runReduceByWorker(&reply, reducef)
			case Wait:
				time.Sleep(time.Second * 5)
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ApplyForTask() (ApplyTaskReply, bool) {
	args := ApplyTaskArgs{}
	reply := ApplyTaskReply{}
	args.MessageType = TaskApply
	ok := call("Coordinator.RunTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		return reply, true
	} else {
		////fmt.Printf("call failed!\n")
	}
	return ApplyTaskReply{}, false
}

func WorkerFinishWork(taskType TaskTypes, taskId int, filePath []string) {
	args := ApplyTaskArgs{
		TaskType:    taskType,
		TaskId:      taskId,
		FilePath:    filePath,
		MessageType: TaskFinish,
	}
	reply := ApplyTaskReply{}
	call("Coordinator.RunTask", &args, &reply)
	//if ok {
	//	// reply.Y should be 100.
	////	fmt.Println("send task-finished message success")
	//} else {
	////	fmt.Printf("call failed!\n")
	//}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
	ok := call("Coordinator.RunTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		//fmt.Printf("call failed!\n")
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
		//log.Fatal("dialing:", err)-
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
