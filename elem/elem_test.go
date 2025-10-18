package elem

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var id0 = NewId(0, 4)
var id1 = NewId(1, 4)
var id2 = NewId(2, 4)
var id3 = NewId(3, 4)

var id10 = id1.Add2(0, 2)
var id11 = id1.Add2(1, 2)

var id20 = id2.Add2(0, 2)
var id21 = id2.Add2(1, 2)

var id210 = id21.Add2(0, 2)
var id211 = id21.Add2(1, 2)

func gather(elems Elements) []any {
	var values []any

	for e := range elems.Values {
		fmt.Printf("%d\n", e)
		values = append(values, e)
	}
	return values
}

type Pair[A, B any] struct {
	A A
	B B
}

func AsPair[A, B any](a A, b B) Pair[A, B] {
	return Pair[A, B]{a, b}
}

var _ = Describe("Elements", func() {
	Context("multi", func() {
		It("basic handling", func() {
			elems := NewElements()

			elems.Add(NewElement(id0, 1))
			Expect(elems.IsComplete()).To(BeFalse())
			elems.Add(NewElement(id10, 2))
			Expect(elems.IsComplete()).To(BeFalse())
			elems.Add(NewElement(id11, 3))
			Expect(elems.IsComplete()).To(BeFalse())
			elems.Add(NewElement(id20, 4))
			Expect(elems.IsComplete()).To(BeFalse())
			elems.Add(NewElement(id210, 5))
			Expect(elems.IsComplete()).To(BeFalse())
			elems.Add(NewElement(id211, 6))
			Expect(elems.IsComplete()).To(BeFalse())
			elems.Add(NewElement(id3, 7))
			Expect(elems.IsComplete()).To(BeTrue())
			values := gather(elems)
			Expect(values).To(Equal([]any{1, 2, 3, 4, 5, 6, 7}))

			elems.SetKnown()
			it := elems.Iterator()
			var ivalues []any
			for {
				if b, _ := it.HasNext(); !b {
					break
				}
				ivalues = append(ivalues, it.Next().V().(int))
			}
			Expect(ivalues).To(Equal(values))

		})

		It("incomplete flat iterator", func() {
			elems := NewElements()
			it := elems.Iterator()
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(true))
			elems.Add(NewElement(id1, 2))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			elems.Add(NewElement(id2, 3))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id0, 1))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(1))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(2))

			it.HasNext() // repeatable
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(3))

			elems.Add(NewElement(id3, 4))
			it.HasNext()
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(4))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, true)))
			Expect(it.HasEndReached()).To(Equal(true))
		})

		It("incomplete deep iterator", func() {
			elems := NewElements()
			it := elems.Iterator()
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(true))

			elems.Add(NewElement(id11, 3))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id0, 1))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(1))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id10, 2))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(2))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(3))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id2, 4))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(4))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id3, 5))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(5))
			Expect(it.HasEndReached()).To(Equal(true))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, true)))
			Expect(it.HasEndReached()).To(Equal(true))
		})
	})

	Context("single", func() {
		It("basic flat handling", func() {
			elems := NewSingle(id1)
			it := elems.Iterator()

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id1, 1))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.Next().V()).To(Equal(1))
			Expect(it.HasEndReached()).To(Equal(true))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, true)))

			values := gather(elems)
			Expect(values).To(Equal([]any{1}))
		})

		It("basic nested handling", func() {
			elems := NewSingle(id1)
			elems.Add(NewElement(id10, 1))
			elems.Add(NewElement(id11, 2))

			values := gather(elems)
			Expect(values).To(Equal([]any{1, 2}))

			elems.SetKnown()
			it := elems.Iterator()
			var ivalues []any
			for {
				if b, _ := it.HasNext(); !b {
					break
				}
				ivalues = append(ivalues, it.Next().V().(int))
			}
			Expect(ivalues).To(Equal(values))
		})

		It("incomplete deep iterator", func() {
			elems := NewSingle(id1)
			it := elems.Iterator()
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id11, 2))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, false)))
			Expect(it.HasEndReached()).To(Equal(false))

			elems.Add(NewElement(id10, 1))
			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(1))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(true, false)))
			Expect(it.HasEndReached()).To(Equal(false))
			Expect(it.Next().V()).To(Equal(2))

			Expect(AsPair(it.HasNext())).To(Equal(AsPair(false, true)))
			Expect(it.HasEndReached()).To(Equal(true))
		})
	})
})
