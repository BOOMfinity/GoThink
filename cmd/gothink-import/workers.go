package main

import (
	"runtime"
	"sync"
)

func newWorker() *worker {
	return &worker {
		receiver: make(chan *workerData, 5000),
	}
}

type workerData struct {
	handler func(x interface{})
	data interface{}
}

type worker struct {
	jobs uint64
	receiver chan *workerData
}

func (w *worker) run(finished chan<- bool) {
	for job := range w.receiver {
		job.handler(job.data)
		w.jobs--
		finished <- true
	}
}

func newWorkerPool() *workerPool {
	return &workerPool {
		finished: make(chan bool, 5000),
		WaitGroup: new(sync.WaitGroup),
		mutex: new(sync.Mutex),
	}
}

type workerPool struct {
	workers []*worker
	*sync.WaitGroup
	mutex *sync.Mutex
	finished chan bool
}

func (wp *workerPool) AddJob(job func(x interface{}), data interface{}) {
	var wo *worker
	wp.mutex.Lock()
	for _, w := range wp.workers {
		if wo == nil {
			wo = w
			continue
		}
		if w.jobs < wo.jobs {
			wo = w
		}
	}
	wp.mutex.Unlock()
	if wo == nil {
		return
	}
	wp.Add(1)
	wo.receiver <- &workerData {
		handler: job,
		data: data,
	}
	wo.jobs++
}

func (wp *workerPool) loop() {
	for range wp.finished {
		wp.Done()
	}
}

func (wp *workerPool) Spawn(num int) {
	go wp.loop()
	if num == 0 {
		num = runtime.NumCPU()
	}
	i := 0
	for i < num {
		w := newWorker()
		go w.run(wp.finished)
		wp.workers = append(wp.workers, w)
		i++
	}
}
