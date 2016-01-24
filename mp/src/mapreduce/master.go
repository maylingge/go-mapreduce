package master

import (
	"os"
    "log"
    "io/ioutil"
    "bufio"
    "bytes"
    "strings"
    "io"
	"util"
	"net"
	"net/http"
)

type Master struct {
	workersize 	int64
	input		string
	output		string
	logger		*log.Logger	
	reducekeys	[]string
	addr		string
	workerchan  []chan util.WorkerInfo
}

const (
	MAX = 10
)

func New(logger *log.Logger, input string, output string) *Master {
	
	m := &Master{logger:logger, 
						input:input, 
						output: output, 
						reducekeys: []string{"l", "m", "a"}, workerchan: make(chan util.WorkerInfo, MAX)}
	rpc.Register(m)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		m.logger.Fatal(err)
	}
	go http.Serve(l, nil)
	m.logger.Info("Master is running")
	return m
}


func (m *Master) Register(worker *util.WorkerInfo, result *int) err {
	m.logger.Info("Registering worker: " + worker.Addr)
	workerchan <- worker
	*result = 0	
	return nil
}

func (m *Master) Split() {
	file, err := os.Open(m.input)
	defer file.Close()

	if err != nil {
		m.logger.Fatal(err)
	}		
	
	fileinfo, _ := file.Stat()
	filesize := fileinfo.Size()

	splitsize := filesize/int64(m.workersize)
	m.logger.Print(splitsize)

	for i:=int64(0);i<m.workersize;i++ {
		size := splitsize
		if i == m.workersize-1 {	
			size = filesize - i*splitsize
		}

		partBuffer := make([]byte, size)
		file.Read(partBuffer)
		tmpfile := util.Get_tmpfile(m.input, i)
		_, err := os.Create(tmpfile)
		if err != nil {
			m.logger.Fatal(err)
		}

		ioutil.WriteFile(tmpfile, partBuffer, os.ModeAppend)
		m.logger.Print("Split to file:" + tmpfile)
	}
}

func (m *Master) Merge() {
	fo, err := os.Create(m.output)
	if err != nil {
		m.logger.Fatal(err)
	}
	defer fo.Close()

	
	buf := make([]byte, 1024)
	for i := range m.reducekeys {
				fi, err := os.Open(util.Get_reduce_resultfile(m.input,int64(i)))
				if err != nil {
					m.logger.Fatal(err) 
				} 	
				defer fi.Close()
				for {
						n, err := fi.Read(buf)
						if err != nil && err != io.EOF {
							m.logger.Fatal(err)
						}
						
						if n == 0 {
							break
						}

						_, err = fo.Write(buf[:n])
						if err != nil {
							m.logger.Fatal(err)
						}
				}

	}
}

func (m *Master) MR() {
	m.Split()
	m.Map()
	m.Reduce()
	m.Merge()
}
