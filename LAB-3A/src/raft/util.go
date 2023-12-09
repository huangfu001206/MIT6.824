package raft

import (
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Debugging
const Debug = false

func PrintToFile(format string, a ...interface{}) {
	var mutex sync.Mutex
	mutex.Lock()
	file, err := os.OpenFile("./test.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// 设置log输出到文件
	log.SetOutput(file)

	// 使用log.Printf输出日志
	log.Printf(format, a...)
	mutex.Unlock()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		//log.Printf(format, a...)
		PrintToFile(format, a...)
	}
	return
}

func GetRandEleInrTime() time.Duration {
	ms := voteInrTimeLo + (rand.Int63() % (voteInrTimeHi - voteInrTimeLo))
	return time.Duration(ms) * time.Millisecond
}

func GetHeartbeatInrTime() time.Duration {
	return heartbeatInrTime * time.Millisecond
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
