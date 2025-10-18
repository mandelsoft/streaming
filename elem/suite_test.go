package elem_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"testing"
)

func TestIterutils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Elem Suite")
}
