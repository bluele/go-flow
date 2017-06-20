package flow

import (
	"fmt"

	"github.com/awalterschulze/gographviz"
)

var GraphName = "flow"

// GetGraphString returns a graph string string which is written by dot language
func GetGraphString(tk Task) string {
	graph := newGraph(fmt.Sprintf(`digraph %v {}`, GraphName))
	walk(newgraphEdgeCache(graph), newgraphNodeCache(graph), tk.(*task))
	return graph.String()
}

func newGraph(buf string) *gographviz.Graph {
	graphAst, _ := gographviz.ParseString(buf)
	graph := gographviz.NewGraph()
	gographviz.Analyse(graphAst, graph)
	return graph
}

type graphNode struct {
	*task
	graphEdges []*graphEdge
}

func (node *graphNode) Equal(other *graphNode) bool {
	return node.task == other.task
}

func (node *graphNode) AddEdge(edge *graphEdge) {
	for _, e := range node.graphEdges {
		if edge.Equal(e) {
			return
		}
	}
	node.graphEdges = append(node.graphEdges, edge)
}

type nodeCache struct {
	cc    []*graphNode
	graph *gographviz.Graph
}

func newgraphNodeCache(graph *gographviz.Graph) *nodeCache {
	return &nodeCache{
		cc:    []*graphNode{},
		graph: graph,
	}
}

func (nc *nodeCache) Get(tk *task) (*graphNode, bool) {
	for _, c := range nc.cc {
		if c.task == tk {
			return c, false
		}
	}
	node := &graphNode{task: tk}
	nc.cc = append(nc.cc, node)
	nc.graph.AddNode(GraphName, tk.Name(), nil)
	return node, true
}

type graphEdge struct {
	Output Output
	Parent *graphNode
	Child  *graphNode
}

func (edge *graphEdge) Equal(other *graphEdge) bool {
	return edge.Child == other.Child && edge.Parent == other.Parent && edge.Output == other.Output
}

type edgeCache struct {
	cc    []*graphEdge
	graph *gographviz.Graph
}

func newgraphEdgeCache(graph *gographviz.Graph) *edgeCache {
	return &edgeCache{
		cc:    []*graphEdge{},
		graph: graph,
	}
}

func (ec *edgeCache) Get(cnode, pnode *graphNode, out Output) (*graphEdge, bool) {
	edge := &graphEdge{
		Output: out,
		Parent: pnode,
		Child:  cnode,
	}
	for _, c := range ec.cc {
		if c.Equal(edge) {
			return c, false
		}
	}
	ec.graph.AddEdge(pnode.Name(), cnode.Name(), true, map[string]string{
		"label": fmt.Sprintf("%#v", out.String()),
	})
	ec.cc = append(ec.cc, edge)
	return edge, true
}

func walk(ec *edgeCache, nc *nodeCache, tk *task) {
	node, _ := nc.Get(tk)
	for _, in := range tk.inputs {
		parent := in.(*taskInput).tk
		pout := in.(*taskInput).Output
		pnode, _ := nc.Get(parent)
		edge, ok := ec.Get(node, pnode, pout)
		if !ok {
			panic(fmt.Errorf("dupliacte reference: %v(%v) => %v", pnode.Name(), pout.String(), node.Name()))
		}
		node.AddEdge(edge)
		walk(ec, nc, parent)
	}
}
