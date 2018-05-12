package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup

	addrCh := make(chan string)
	taskCh := make(chan int, ntasks)

	go func() {
		for {
			addr := <- registerChan
			addrCh <- addr
		}
	}()

	for i := 0; i < ntasks; i++ {
		taskCh <- i
	}

	n := int32(ntasks)

	switch phase {
	case mapPhase:
		for i := range taskCh {
			addr := <- addrCh

			wg.Add(1)
			go func(addr string, i int) {
				ok := call(addr, "Worker.DoTask", DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}, nil)
				wg.Done()

				if !ok {
					taskCh <- i
				} else {
					if atomic.AddInt32(&n, -1) == 0 {
						close(taskCh)
					}
				}

				addrCh <- addr

			}(addr, i)
		}
	case reducePhase:
		for i := range taskCh {
			addr := <- addrCh

			wg.Add(1)
			go func(addr string, i int) {
				ok := call(addr, "Worker.DoTask", DoTaskArgs{jobName, "", phase, i, n_other}, nil)
				wg.Done()

				if !ok {
					taskCh <- i
				} else {
					if atomic.AddInt32(&n, -1) == 0 {
						close(taskCh)
					}
				}

				addrCh <- addr
			}(addr, i)
		}
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
