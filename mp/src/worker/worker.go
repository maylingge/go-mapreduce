package worker
import (
	"log"
	"util"
	"net/rpc"
	"net"
	"net/http"
	"bytes"
	"bufio"
	"os"
	"strings"
	"strconv"
	"io/ioutil"
)

type Worker interface {
	Map(args *util.MapInput, reply *int) error
	Reduce(args *util.ReduceInput, reply *int) error
}

type DefaultWorker struct {
	workerinfo	util.WorkerInfo
	logger		*log.Logger
}

func New(workerinfo util.WorkerInfo, logger *log.Logger) Worker {
	w := &DefaultWorker{ workerinfo: workerinfo, logger: logger}
	rpc.Register(w)	
	//rpc.HandleHTTP()	
	l, err := net.Listen("tcp", workerinfo.Addr)
	if err != nil {
		w.logger.Fatal(err)
	}
	go http.Serve(l, nil)

	client, err := rpc.DialHTTP("tcp", w.workerinfo.Master)
	if err != nil {
		w.logger.Fatal(err)
	}
	var reply int
	args := &w.workerinfo
	err = client.Call("Master.Register", args, &reply)
	if err != nil {
		w.logger.Fatal(err)
	}
	
	w.logger.Println(w.workerinfo.Name + " is running")
	return w
}

func (m *DefaultWorker) Map(args *util.MapInput, reply *int) error {
	*reply = 1
	m.logger.Println("Job is running : " + strconv.FormatInt(args.JobID, 10))
	resultfile := util.Get_map_resultfile(args.Source, args.JobID)
	_, err := os.Create(resultfile)
	if err != nil {
		m.logger.Println(err)
		return err
	}
	var buf bytes.Buffer

	infile, err := os.Open(util.Get_tmpfile(args.Source, args.JobID))
	defer infile.Close()
	if err != nil {
        m.logger.Println(err)
		return err
    }
	scanner := bufio.NewScanner(infile)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		buf.WriteString(scanner.Text() + ":1\n") 
	}
	if err := scanner.Err(); err != nil {
		m.logger.Println(err)
		return err
	}
	ioutil.WriteFile(resultfile, buf.Bytes(), os.ModeAppend)
	*reply = 0
	return nil
}

// Master should tell Reduce where are the mapresult
// each Reduce only handles part of the result
func (m *DefaultWorker) Reduce(args *util.ReduceInput, reply *int) error {
	*reply = 1
	resmap := make(map[string]int)
	for _, result := range  args.Mapresult {
		infile, err := os.Open(result)
		if err != nil {
			m.logger.Println(err)
			return err
		}
		defer infile.Close()
		scanner := bufio.NewScanner(infile)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, args.Key) {
				word := strings.Split(line, ":")[0]
				_, ok := resmap[word]
				if ok {
					resmap[word] += 1
				} else {
					resmap[word] = 1
				}
			}
		}
	}
	
	os.Create(args.Result)
 	var buf bytes.Buffer	
	for key, value := range resmap {
		buf.WriteString(key + ":" + strconv.FormatInt(int64(value), 10) + "\n")
	}
	ioutil.WriteFile(args.Result, buf.Bytes(), os.ModeAppend)
	*reply = 0
	return nil
}


