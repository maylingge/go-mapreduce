package main

import (
	"logger"
	"master"
	"os"
	"util"
	"worker"
)


func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		logger.Logger.Fatal("Only two arguments expected!")
	}
	logger.Logger.Println("Here we go!")
	m := master.New(logger.Logger, args[0], args[1])
	logger.Logger.Println(m.Addr)
	
	w1 := util.WorkerInfo{":2345", "worker1", m.Addr} 
	worker.New(w1, logger.Logger)

	w2 := util.WorkerInfo{":3456", "worker2", m.Addr} 
	worker.New(w2, logger.Logger)

	w3 := util.WorkerInfo{":4567", "worker3", m.Addr} 
	worker.New(w3, logger.Logger)
	
	m.MR()
}


