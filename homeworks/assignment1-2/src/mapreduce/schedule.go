package mapreduce

import (
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			args := &DoTaskArgs{
				JobName:       mr.jobName,
				Phase:         phase,
				File:          mr.files[idx],
				TaskNumber:    idx,
				NumOtherPhase: nios,
			}
			wk := <-mr.registerChannel
			for !call(wk, "Worker.DoTask", args, nil) {
				wk = <-mr.registerChannel
			}
			go func() {
				mr.registerChannel <- wk
			}()
		}(i)
	}
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
