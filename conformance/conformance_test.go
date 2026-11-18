// The upstream Gateway API conformance suite, run against EdgeRush (docs/04 §7). Nothing
// of EdgeRush's own is here: the class, the profiles and the report's fields come in as the
// suite's flags, from kind/conformance.sh, which builds this into the runner image
// (Dockerfile) and runs it as a Pod in a kind cluster.
package conformance_test

import (
	"testing"

	"sigs.k8s.io/gateway-api/conformance"
)

func TestConformance(t *testing.T) {
	conformance.RunConformance(t)
}
