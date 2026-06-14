package workflow

// Step is a single shell command in a workflow.
type Step struct {
	Command string
}

type Workflow struct {
	Name  string
	Steps []Step
}

type Registry struct {
	workflows map[string]Workflow
}

func NewRegistry() *Registry {
	return &Registry{workflows: make(map[string]Workflow)}
}

func (r *Registry) Register(w Workflow) {
	r.workflows[w.Name] = w
}

func (r *Registry) Get(name string) (Workflow, bool) {
	w, ok := r.workflows[name]
	return w, ok
}

func (r *Registry) List() []Workflow {
	list := make([]Workflow, 0, len(r.workflows))
	for _, w := range r.workflows {
		list = append(list, w)
	}
	return list
}
