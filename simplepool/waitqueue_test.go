package simplepool

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func WaitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		fmt.Printf("wg done\n")
		close(done)
	}()
	return done
}

var _ = Describe("WaitQueue", func() {
	var queue WaitQueue[string]

	BeforeEach(func() {
		queue = WaitQueue[string]{}
	})

	It("should handle basic push and pop operations", func() {
		queue.Push("test")
		v, ok := queue.Consume(nil)
		Expect(ok).To(Equal(true))
		Expect(v).To(Equal("test"))
	})

	It("should maintain FIFO order", func() {
		queue.Push("first")
		queue.Push("second")
		queue.Push("third")

		v, ok := queue.Consume(nil)
		Expect(ok).To(Equal(true))
		Expect(v).To(Equal("first"))

		v, ok = queue.Consume(nil)
		Expect(ok).To(Equal(true))
		Expect(v).To(Equal("second"))

		v, ok = queue.Consume(nil)
		Expect(ok).To(Equal(true))
		Expect(v).To(Equal("third"))
	})

	It("should wait when queue is empty", func() {
		done := make(chan bool)
		go func() {
			v, ok := queue.Consume(nil)
			Expect(ok).To(Equal(true))
			Expect(v).To(Equal("first"))
			v, ok = queue.Consume(nil)
			Expect(ok).To(Equal(true))
			Expect(v).To(Equal("second"))
			done <- true
		}()

		time.Sleep(100 * time.Millisecond)
		queue.Push("first")
		time.Sleep(100 * time.Millisecond)
		queue.Push("second")
		Eventually(done).Should(Receive())
	})

	It("canceles single request", func() {
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			_, ok := queue.Consume(ctx)
			Expect(ok).To(Equal(false))
			wg.Done()
		}()
		go func() {
			v, ok := queue.Consume(nil)
			Expect(ok).To(Equal(true))
			Expect(v).To(Equal("first"))
			wg.Done()
		}()

		time.Sleep(100 * time.Millisecond)
		cancel()

		time.Sleep(100 * time.Millisecond)
		queue.Push("first")

		Eventually(WaitGroupDone(&wg), 5000*time.Millisecond).Should(BeClosed())
	})
})
