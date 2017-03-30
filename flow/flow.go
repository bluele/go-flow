package flow

import "sync"

// Run resolves the dependency of the specified task and starts it
func Run(tk Task) {
	wg := new(sync.WaitGroup)
	run(wg, []Task{tk})
	wg.Wait()
}

func run(wg *sync.WaitGroup, tks []Task) {
	for _, tk := range tks {
		if tk.isDone() {
			Logger.Printf("task '%v' is already done, skip this\n", tk.Name())
			tk.skip()
		} else {
			wg.Add(1)
			go func(tk Task) {
				defer wg.Done()
				defer func(tk Task) {
					if err := recover(); err != nil {
						Logger.Printf("task '%v' got an error %v\n", tk.Name(), err)
						tk.destroy()
					}
				}(tk)
				Logger.Printf("task '%v' is ready?\n", tk.Name())
				<-tk.ready()
				Logger.Printf("task '%v' is started\n", tk.Name())
				tk.run()
			}(tk)
			run(wg, tk.Requires())
		}
	}
	return
}
