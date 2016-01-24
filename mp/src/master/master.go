package master

import (
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"util"
)

type Master struct {
	workersize int64
	input      string
	output     string
	logger     *log.Logger
	reducekeys []string
	Addr       string
	workerchan chan *util.WorkerInfo
	mapresult  []string
}

const (
	MAX = 10
)

func New(logger *log.Logger, input string, output string) *Master {

	m := &Master{logger: logger,
		input:      input,
		output:     output,
		reducekeys: []string{"l", "m", "a"},
		workerchan: make(chan *util.WorkerInfo, MAX),
		Addr:       ":1234",
		workersize: 0,
	}
	rpc.Register(m)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", m.Addr)
	if err != nil {
		m.logger.Fatal(err)
	}
	go http.Serve(l, nil)
	m.logger.Println("Master is running")
	return m
}

func (m *Master) Register(worker *util.WorkerInfo, result *int) error {
	m.logger.Println("Registering worker: " + worker.Addr)
	m.workerchan <- worker
	m.workersize += 1
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

	splitsize := filesize / int64(m.workersize)
	m.logger.Print(splitsize)

	for i := int64(0); i < m.workersize; i++ {
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
		fi, err := os.Open(util.Get_reduce_resultfile(m.input, int64(i)))
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

func (m *Master) RunJob() {
	var wg sync.WaitGroup
	jobs := make(chan int64)
	go func() {
		for i := int64(0); i < m.workersize; i++ {
			jobs <- i
		}
	}()
	count := 0
	for i := range jobs {
		w := <-m.workerchan
		m.mapresult = append(m.mapresult, util.Get_map_resultfile(m.input, int64(i)))
		wg.Add(1)
		go func(jobid int64, w *util.WorkerInfo) {
			defer wg.Done()
			client, err := rpc.DialHTTP("tcp", w.Addr)
			if err != nil {
				m.logger.Fatal(err)
			}
			var reply int
			input := &util.MapInput{m.input, jobid}
			err = client.Call("DefaultWorker.Map", input, &reply)
			if err != nil {
				m.logger.Println(err)
				jobs <- jobid
				return
			}
			count += 1
			m.workerchan <- w
			if int64(count) == m.workersize {
				close(jobs)
			}
		}(i, w)
	}
	wg.Wait()

	keys := make(chan string)

	go func() {
		for _, key := range m.reducekeys {
			keys <- key
		}
	}()

	i := 0
	count = 0
	for key := range keys {
		w := <-m.workerchan
		wg.Add(1)
		go func(key string, i int, w *util.WorkerInfo) {
			defer wg.Done()
			client, err := rpc.DialHTTP("tcp", w.Addr)
			if err != nil {
				m.logger.Fatal(err)
			}
			var reply int
			err = client.Call("DefaultWorker.Reduce", &util.ReduceInput{m.mapresult, key, util.Get_reduce_resultfile(m.input, int64(i))}, &reply)
			if err != nil {
				m.logger.Println(err)
				keys <- key
				return
			}
			m.workerchan <- w
			count += 1
			if count == len(m.reducekeys) {
				close(keys)
			}

		}(key, i, w)
		i += 1
	}
	wg.Wait()
	m.logger.Println("MR done")
}

func (m *Master) MR() {
	m.Split()
	m.RunJob()
	m.Merge()
}
