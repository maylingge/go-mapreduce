package util
import (
	"strconv"
)

type WorkerInfo struct {
	Addr	string
	Name	string
	Master	string
}

type ReduceInput struct {
	Mapresult []string
	Key			string
	Result		string
}

type MapInput 	struct {
	Source	string
	JobID	int64
}
func Get_tmpfile(source string, i int64) string {
	return source + ".tmp." + strconv.FormatInt(i, 10)
}

func Get_map_resultfile(source string, i int64) string {
	return Get_tmpfile(source, i) + ".map"
}

func Get_reduce_resultfile(source string, i int64) string {
	return Get_tmpfile(source, i) + ".reduce"
}
