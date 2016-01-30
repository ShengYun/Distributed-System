package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	jobType JobType
	jobNum  int
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// register workers and dispatch initial jobs
	go func() {
		for workerAddressToRegister := range mr.registerChannel {
			fmt.Printf("Got registration request from %s\n", workerAddressToRegister)
			mr.Workers[workerAddressToRegister] = &WorkerInfo{workerAddressToRegister, Idle, -1}
			fmt.Printf("Finished registration %s\n", workerAddressToRegister)
			fmt.Println("dispatching from register function")
			go mr.dispatchJob(mr.Workers[workerAddressToRegister])
		}
	}()

	for worker := range mr.statusChannel {
		fmt.Printf("Master: Got status report from worker %s, job %d of %s has done\n", worker.address, worker.jobNum, worker.jobType)
		mr.logJob(worker)
		if len(mr.reduceDone) == mr.nReduce && mr.allReduceJobsDone() == -1 {
			break
		}
		go mr.dispatchJob(worker)
	}
	//gather worker informations
	return mr.KillWorkers()
}

func (mr *MapReduce) logJob(worker *WorkerInfo) {
	if worker.jobType == Map {
		mr.mapDone[worker.jobNum] = true
	} else {
		mr.reduceDone[worker.jobNum] = true
	}
}

func (mr *MapReduce) allMapJobsDone() int {
	for i := 0; i < mr.nMap; i++ {
		if finished := mr.mapDone[i]; !finished {
			return i
		}
	}
	return -1
}

func (mr *MapReduce) allReduceJobsDone() int {
	for i := 0; i < mr.nReduce; i++ {
		if finished := mr.reduceDone[i]; !finished {
			return i
		}
	}
	return -1
}

func (mr *MapReduce) dispatchJob(worker *WorkerInfo) {
	var jobNum int
	var jobtype JobType
	if len(mr.mapDone) == mr.nMap {
		//wait for all dispatched map jobs done
		for mr.allMapJobsDone() != -1 {
			fmt.Println("waiting for all map jobs to finish")
		}
		jobtype = Reduce
	} else {
		jobtype = Map
	}
	args := &DoJobArgs{}
	if jobtype == Map {
		args.NumOtherPhase = mr.nReduce
		for i := 0; i < mr.nMap; i++ {
			if _, ok := mr.mapDone[i]; !ok {
				jobNum = i
				break
			}
		}
		//job dispathced, not done yet
		mr.mapDone[jobNum] = false
	} else {
		args.NumOtherPhase = mr.nMap
		for i := 0; i < mr.nReduce; i++ {
			if _, ok := mr.reduceDone[i]; !ok {
				jobNum = i
				break
			}
		}
		//job dispatched, not done yet
		mr.reduceDone[jobNum] = false
	}
	fmt.Printf("Master: Dispatching %s job %d to worker %s\n", jobtype, jobNum, worker.address)
	args.File = mr.file
	args.JobNumber = jobNum
	args.Operation = jobtype
	worker.jobType = jobtype
	worker.jobNum = jobNum
	var reply DoJobReply
	ok := call(worker.address, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("Master: Error dispatching %v job to worker %s, worker failed, need to redispatch the job\n", jobtype, worker.address)
		if jobtype == Map {
			delete(mr.mapDone, jobNum)
		} else {
			delete(mr.reduceDone, jobNum)
		}
	}
}
