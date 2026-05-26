package main

import (
	"fmt"
	"strconv"
)

// optionalInt64 is a flag.Value that produces a *int64. nil when the
// flag wasn't passed; non-nil (possibly pointing to 0) when it was.
// Used so the CLI can pass `Inputs.UID = nil` when the operator
// didn't supply --uid, vs `Inputs.UID = &(0)` for the explicit
// "set to zero" case (though zero is rejected by the operator
// permissive-mode contract).
type optionalInt64 struct {
	value *int64
}

func (o *optionalInt64) Set(s string) error {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid integer %q: %w", s, err)
	}
	o.value = &n
	return nil
}

func (o *optionalInt64) String() string {
	if o == nil || o.value == nil {
		return ""
	}
	return strconv.FormatInt(*o.value, 10)
}

// ptr returns the wrapped *int64. Caller should treat the result as
// read-only; modifying *ptr after Set is unspecified behavior.
func (o *optionalInt64) ptr() *int64 {
	if o == nil {
		return nil
	}
	return o.value
}
