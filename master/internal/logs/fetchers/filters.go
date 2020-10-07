package fetchers

import (
	"errors"
	"fmt"
	"github.com/determined-ai/determined/proto/pkg/filters"
)

var FilterError = errors.New("unsupported filter")

func NewUnsupportedFieldError(f *filters.Filter) error {
	return fmt.Errorf("unsupported field in filter on %s: %w", f.Field, FilterError)
}

func NewUnsupportedOperationError(f *filters.Filter) error {
	return fmt.Errorf("unsupported operation %s for filter on %s: %w", f.Operation, f.Field, FilterError)
}

func NewMissingValuesError(f *filters.Filter) error {
	return fmt.Errorf("missing arguments for filter on %s: %w", f.Field, FilterError)
}

func NewTooManyValuesError(f *filters.Filter) error {
	return fmt.Errorf("wrong number of arguments for filter on %s: %w", f.Field, FilterError)
}

func NewUnsupportedValuesError(f *filters.Filter) error {
	return fmt.Errorf("unsupported values %T for filter on %s: %w", f.Values, f.Field, FilterError)
}