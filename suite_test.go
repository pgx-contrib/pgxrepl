package pgxrepl_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgxrepl(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxrepl Suite")
}
