package mapreduce

import (
	"encoding/json"
	"hash/fnv"
<<<<<<< HEAD
	"io/ioutil"
	"os"
=======
	"log"
>>>>>>> c94de3443a34677b805d9dd82d9eaefeaa6c690e
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	bytes, err := ioutil.ReadFile(inFile)
	checkError(err)

	kvs := mapF(inFile, string(bytes))

	reduceFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceFiles[i], err = os.Create(reduceName(jobName, mapTaskNumber, i))
		checkError(err)
		defer reduceFiles[i].Close()
		encoders[i] = json.NewEncoder(reduceFiles[i])
	}

	for _, kv := range kvs {
		idx := ihash(kv.Key) % uint32(nReduce)
		err := encoders[idx].Encode(&kv)
		checkError(err)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	checkError(err)
	return h.Sum32()
}
