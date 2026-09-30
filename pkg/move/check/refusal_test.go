package check

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefuse(t *testing.T) {
	cause := errors.New("cause")
	err := refuse(cause)
	require.EqualError(t, err, "cause")
	require.ErrorIs(t, err, ErrRefused)
	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, cause, ErrRefused)
}
