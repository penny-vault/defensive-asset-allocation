package daa_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDefensiveAssetAllocation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Defensive Asset Allocation Suite")
}
