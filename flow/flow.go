package flow

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/awalterschulze/gographviz"
)

type Flow struct {
	entry   Task
	buffers []Input
}

type Stats struct {
	Metrics []*Metric
}

type Metric struct {
	Name       string
	Time       time.Time
	BufferSize int
}

func (fl *Flow) Stats() *Stats {
	stats := new(Stats)
	for _, in := range fl.buffers {
		now := time.Now()
		m := &Metric{
			Name:       in.String(),
			Time:       now,
			BufferSize: len(in.Channel()),
		}
		stats.Metrics = append(stats.Metrics, m)
	}
	return stats
}

func (fl *Flow) StatsHandler(w http.ResponseWriter, r *http.Request) {
	j, err := json.Marshal(fl.Stats())
	if err == nil {
		fmt.Fprint(w, string(j))
	} else {
		panic(err)
	}
}

func (fl *Flow) Run() (*Result, error) {
	rs := newResult()
	fl.run(rs, nil, []Input{&taskInput{tk: fl.entry.(*task)}})
	rs.wg.Wait()
	return rs, nil
}

func New(tk Task) *Flow {
	return &Flow{entry: tk}
}

// Run resolves the dependency of the specified task and starts it
func Run(tk Task) (*Result, error) {
	return New(tk).Run()
}

type Result struct {
	wg    *sync.WaitGroup
	mu    sync.Mutex
	graph *gographviz.Graph
}

func newResult() *Result {
	return &Result{
		wg:    new(sync.WaitGroup),
		graph: newGraph(fmt.Sprintf(`digraph %v {}`, GraphName)),
	}
}

// Graph returns graph string
func (rs *Result) Graph() string {
	return rs.graph.String()
}

func (fl *Flow) run(rs *Result, child Task, ins []Input) {
	for _, in := range ins {
		fl.buffers = append(fl.buffers, resolveDependentInputs(in)...)
		for _, tk := range in.(TaskInput).Tasks() {
			if child != nil {
				rs.graph.AddEdge(
					escapeString(tk.Name()),
					escapeString(child.Name()),
					true,
					map[string]string{
						"label": escapeString(in.(TaskInput).String()),
					})
			}
			if tk.isDone() {
				continue
			}
			tk.setDone()
			if tk.isSkip() {
				Logger.Printf("Task '%v' is already done, skip this\n", tk.Name())
				tk.skip()
				rs.graph.AddNode(
					GraphName,
					escapeString(tk.Name()),
					map[string]string{
						"label": fmt.Sprintf("%#v", fmt.Sprintf("%v\n(skipped)", tk.Name())),
					})
				continue
			}

			rs.wg.Add(1)
			go func(tk Task) {
				defer rs.wg.Done()
				defer func(tk Task) {
					if err := recover(); err != nil {
						Logger.Printf("Task '%v' got an error %v\n", tk.Name(), err)
						tk.destroy()
					}
				}(tk)
				Logger.Printf("Task '%v' is ready?\n", tk.Name())
				<-tk.ready()
				Logger.Printf("Task '%v' is started\n", tk.Name())
				started := time.Now()
				tk.run()
				et := time.Since(started).String()
				Logger.Printf("Task '%v' is finished. Elapsed time is %v\n", tk.Name(), et)
				rs.graph.AddNode(
					GraphName,
					escapeString(tk.Name()),
					map[string]string{
						"label": fmt.Sprintf("%#v", fmt.Sprintf("%v\ntime:%v", tk.Name(), et)),
					})
			}(tk)
			fl.run(rs, tk, tk.inputs)
		}
	}
	return
}
