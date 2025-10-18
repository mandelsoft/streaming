package simplepool_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"testing"
)

func TestPool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pool Suite")
}
