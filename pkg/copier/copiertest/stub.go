// Package copiertest provides a Copier stub for tests of the runners that
// report copier state, so every runner package shares one definition. It
// lives beside the copier rather than in testutils because the copier's own
// tests import testutils, which a stub there would turn into an import cycle.
package copiertest

import (
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
)

// Stub answers the read-only progress methods of copier.Copier from its
// fields. Every other method is inherited from the embedded nil interface and
// panics if reached, which is the point: a test that needs it is exercising
// more than progress reporting.
type Stub struct {
	copier.Copier
	ETA   status.ETA
	Copy  status.CopyProgress
	Chunk uint64
}

func (s Stub) GetETA() string                    { return s.ETA.String() }
func (s Stub) GetETAState() status.ETA           { return s.ETA }
func (s Stub) GetProgress() string               { return s.Copy.String() }
func (s Stub) CopyProgress() status.CopyProgress { return s.Copy }
func (s Stub) ChunkSize() uint64                 { return s.Chunk }
