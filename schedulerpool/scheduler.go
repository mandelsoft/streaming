package schedulerpool

import (
	"context"
	"github.com/mandelsoft/goutils/general"
	"github.com/mandelsoft/jobscheduler/processors"
	"github.com/mandelsoft/jobscheduler/scheduler"
	"github.com/mandelsoft/streaming/elem"
	"github.com/mandelsoft/streaming/processing"
)

type _pool struct {
	size  int
	sched scheduler.Scheduler
}

var _ processing.Processing = (*_pool)(nil)

func New(ctx context.Context, n int) processing.Processing {
	sched := scheduler.New("streaming")
	sched.AddProcessor(n)
	sched.Run(ctx)
	return &_pool{size: n, sched: sched}
}

func (p *_pool) GetPool() processing.Processing {
	return p
}

func (p *_pool) Size() int {
	return p.size
}

func (p *_pool) Execute(request processing.Request, name ...string) {
	def := scheduler.DefineJob(general.OptionalDefaulted("test", name...), scheduler.RunnerFunc(func(sctx scheduler.SchedulingContext) (scheduler.Result, error) {
		request.Execute(sctx)
		return nil, nil
	}))
	job, err := p.sched.Apply(def)
	if err != nil {
		panic(err)
	}
	job.Schedule()

}

func (p *_pool) Close() error {
	p.sched.Cancel()
	p.sched.Wait()
	return nil
}

func (p *_pool) CreateChannel() processing.Channel {
	return processors.NewChannel[*elem.Element](p.size * 2)
}
