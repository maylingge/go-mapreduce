package mapreduce

import (
	"os"
	"log"
	"strconv"
	"io/ioutil"
	"bufio"
	"bytes"
	"strings"
	"io"
)

type Mapper interface {
	Map(input string, logger *log.Logger)
}

type Reducer interface {
	Reduce(key string, mapresult []string, reducefile string, logger *log.Logger)
}

type DefaultMapper struct {
}

type DefaultReducer struct {
}

type MapReduce struct {
	workersize 	int64
	input		string
	output		string
	logger		*log.Logger	
	mapper		Mapper
	reducer		Reducer
	reducefiles []string
	reducekeys	[]string
}

	

func (m DefaultMapper) Map(input string, logger *log.Logger) {
	logger.Println("Processing file: " + input)
	resultfile := input + ".map"
	_, err := os.Create(resultfile)
	if err != nil {
		logger.Fatal(err)
	}
	var buf bytes.Buffer

	infile, err := os.Open(input)
	defer infile.Close()
	if err != nil {
        logger.Fatal(err)
    }
	scanner := bufio.NewScanner(infile)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		buf.WriteString(scanner.Text() + ":1\n") 
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}
	ioutil.WriteFile(resultfile, buf.Bytes(), os.ModeAppend)
}

// Master should tell Reduce where are the mapresult
// each Reduce only handles part of the result
func (m DefaultReducer) Reduce(key string, mapresult []string, reduceresult string, logger *log.Logger) {
	resmap := make(map[string]int)
	for _, result := range  mapresult {
		infile, err := os.Open(result)
		if err != nil {
			logger.Fatal(err)
		}
		defer infile.Close()
		scanner := bufio.NewScanner(infile)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, key) {
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
	
	os.Create(reduceresult)
 	var buf bytes.Buffer	
	for key, value := range resmap {
		buf.WriteString(key + ":" + strconv.FormatInt(int64(value), 10) + "\n")
	}
	ioutil.WriteFile(reduceresult, buf.Bytes(), os.ModeAppend)
}

func New(logger *log.Logger, input string, workersize int, output string) *MapReduce {
	return & MapReduce{logger:logger, 
						input:input, 
						workersize:int64(workersize), 
						output: output, 
						mapper: DefaultMapper{}, 
						reducer: DefaultReducer{}, 
						reducekeys: []string{"l", "m", "a"}}
}

func (m *MapReduce) Split() {
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
		tmpfile := get_tmpfile(m.input, i)
		_, err := os.Create(tmpfile)
		if err != nil {
			m.logger.Fatal(err)
		}

		ioutil.WriteFile(tmpfile, partBuffer, os.ModeAppend)
		m.logger.Print("Split to file:" + tmpfile)
	}
}

func get_tmpfile(source string, i int64) string {
	return source + ".tmp." + strconv.FormatInt(i, 10)
}

func get_map_resultfile(source string, i int64) string {
	return get_tmpfile(source, i) + ".map"
}

func get_reduce_resultfile(source string, i int64) string {
	return get_tmpfile(source, i) + ".reduce"
}

func (m *MapReduce) Map() {
	for i:=int64(0);i<m.workersize;i++ {
		tmpfile := get_tmpfile(m.input, i)
		m.mapper.Map(tmpfile, m.logger)
	}
}


func (m *MapReduce) Reduce() {
    var	mapresult []string
	for i:=int64(0);i<m.workersize;i++ {
		tmpfile := get_map_resultfile(m.input, i)
		mapresult = append(mapresult, tmpfile)
	}
	
	var reducefiles []string
	for i, key := range m.reducekeys {
		resultfile := get_reduce_resultfile(m.input, int64(i))
		reducefiles = append(reducefiles, resultfile)
		m.reducer.Reduce(key, mapresult, resultfile, m.logger)
	}
}

func (m *MapReduce) Merge() {
	fo, err := os.Create(m.output)
	if err != nil {
		m.logger.Fatal(err)
	}
	defer fo.Close()

	
	buf := make([]byte, 1024)
	for i := range m.reducekeys {
				fi, err := os.Open(get_reduce_resultfile(m.input,int64(i)))
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

func (m *MapReduce) MR() {
	m.Split()
	m.Map()
	m.Reduce()
	m.Merge()
}
