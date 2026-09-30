package check

import "errors"

// ErrRefused matches (with errors.Is) a check failure that is a decision
// rather than an error: the check ran and found a reason to refuse, so a
// retry would be refused again. A check error that does not match it, such
// as a failed query, may be transient. A cutover retries on a transient
// error from its checks under lock, and fails without a retry on a refusal.
var ErrRefused = errors.New("refused by a check")

// refusal marks err as a refusal without changing its message.
type refusal struct{ error }

func (r refusal) Unwrap() []error { return []error{r.error, ErrRefused} }

// refuse marks err as a refusal (see ErrRefused).
func refuse(err error) error { return refusal{err} }
