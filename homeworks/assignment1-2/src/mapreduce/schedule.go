package mapreduce

import "sync"

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
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		args := DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
		wg.Add(1)
		go func(args DoTaskArgs, ch chan string) {
			wk := <-mr.registerChannel
			for {
				if call(wk, "Worker.DoTask", args, nil) {
					go func() {
						mr.registerChannel <- wk
					}()
					break
				}
			}
			wg.Done()
		}(args, mr.registerChannel)
	}
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
