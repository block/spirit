package checksum

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A caller decides whether to retry a checksum from IsReproducible alone, so it
// must hold for the verdicts about the data however they are wrapped, and for
// nothing else.
func TestIsReproducible(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("runner stopped: checksum: %w", err) }
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "differences exhausted", err: ErrDifferencesExhausted, want: true},
		{name: "differences exhausted, wrapped", err: wrap(ErrDifferencesExhausted), want: true},
		{name: "permanent divergence", err: ErrPermanentDivergence, want: true},
		{name: "permanent divergence, wrapped", err: wrap(ErrPermanentDivergence), want: true},
		{name: "permanent divergence, joined", err: errors.Join(context.Canceled, ErrPermanentDivergence), want: true},
		// No divergence is proven when the pass budget runs out, unless every
		// pass repaired, which the checker reports by wrapping both.
		{name: "verification unresolved", err: ErrVerificationUnresolved, want: false},
		{name: "verification unresolved, wrapped", err: wrap(ErrVerificationUnresolved), want: false},
		{
			name: "verification unresolved after a repair on every pass",
			err:  fmt.Errorf("%w; %w", ErrVerificationUnresolved, ErrDifferencesExhausted),
			want: true,
		},
		{name: "attempts exhausted", err: ErrAttemptsExhausted, want: false},
		{name: "attempts exhausted, wrapped", err: wrap(ErrAttemptsExhausted), want: false},
		{name: "cancelled", err: context.Canceled, want: false},
		{name: "unrelated", err: errors.New("connection refused"), want: false},
		{name: "nil", err: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsReproducible(tc.err))
		})
	}
}
