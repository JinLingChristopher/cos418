package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	records := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(filename)
		checkError(err)
		defer f.Close()

		var kv KeyValue
		for decoder := json.NewDecoder(f); decoder.More(); err = decoder.Decode(&kv) {
			checkError(err)
			records[kv.Key] = append(records[kv.Key], kv.Value)
		}
	}

	mf, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkError(err)
	defer mf.Close()

	enc := json.NewEncoder(mf)
	for k, v := range records {
		err = enc.Encode(KeyValue{k, reduceF(k, v)})
		checkError(err)
	}
}
