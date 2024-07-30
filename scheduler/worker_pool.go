package scheduler

func (wp *WorkerPool) worker(workerId int) {
	defer func() {
		wp.wg.Done()
	}()
	for job := range wp.JobChan {
		wp.ResultChan <- job.Executed()
	}
}

func (wp *WorkerPool) Run(FuncProducerJob func(JobChan chan JobInterface)) {
	for i := 0; i < wp.WorkerNum; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	//生产任务
	go func() {
		FuncProducerJob(wp.JobChan)
	}()

	//等待任务执行完毕
	go func() {
		wp.wg.Wait()
		close(wp.ResultChan)
	}()

}
