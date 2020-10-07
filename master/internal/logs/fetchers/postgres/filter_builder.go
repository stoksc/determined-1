package postgres

import (
	"fmt"
	"github.com/determined-ai/determined/master/internal/logs/fetchers"
	"github.com/determined-ai/determined/proto/pkg/filters"
	"github.com/golang/protobuf/ptypes"
	"strconv"
	"strings"
)

type fieldType int

const (
	unsupportedFieldType = iota
	textFieldType
	integerFieldType
	timestampFieldType
)

type fieldToType func(*filters.Filter) fieldType

// filterToSQL takes a filter, the type for the field being filtered and the current paramID for the query it
// is being built into and returns a query fragment, the parameters for that fragment of the query
// and an error if it failed.
func filterToSQL(f *filters.Filter, fType fieldToType, paramID int) (string, interface{}, error) {
	switch fType(f) {
	case textFieldType:
		stringValues, ok := f.Values.(*filters.Filter_StringValues)
		if !ok {
			return "", nil, fetchers.NewUnsupportedValuesError(f)
		}
		values := stringValues.StringValues.Values
		if len(values) < 1 {
			return "", nil, fetchers.NewMissingValuesError(f)
		}
		switch f.Operation {
		case filters.Filter_OPERATION_IN, filters.Filter_OPERATION_NOT_IN:
			tpl := "AND l.%s %s (SELECT unnest($%d::text[])::text)"
			return fmt.Sprintf(tpl, f.Field, opToSQL(f.Operation), paramID), strValuesToSQL(values), nil
		case filters.Filter_OPERATION_EQUAL, filters.Filter_OPERATION_NOT_EQUAL:
			if len(values) > 1 {
				return "", nil, fetchers.NewTooManyValuesError(f)
			}
			tpl := "AND l.%s %s $%d"
			return fmt.Sprintf(tpl, f.Field, opToSQL(f.Operation), paramID), values[0], nil
		default:
			return "", nil, fetchers.NewUnsupportedOperationError(f)
		}
	case integerFieldType:
		intValues, ok := f.Values.(*filters.Filter_IntValues)
		if !ok {
			return "", nil, fetchers.NewUnsupportedValuesError(f)
		}
		values := intValues.IntValues.Values
		if len(values) < 1 {
			return "", nil, fetchers.NewMissingValuesError(f)
		}
		switch f.Operation {
		case filters.Filter_OPERATION_IN, filters.Filter_OPERATION_NOT_IN:
			tpl := "AND l.%s %s (SELECT unnest($%d::integer[])::integer)"
			return fmt.Sprintf(tpl, f.Field, opToSQL(f.Operation), paramID), intValuesToSQL(values), nil
		case filters.Filter_OPERATION_EQUAL, filters.Filter_OPERATION_NOT_EQUAL:
			if len(values) > 1 {
				return "", nil, fetchers.NewTooManyValuesError(f)
			}
			tpl := "AND l.%s %s $%d::integer"
			return fmt.Sprintf(tpl, f.Field, opToSQL(f.Operation), paramID), values[0], nil
		default:
			return "", nil, fetchers.NewUnsupportedOperationError(f)
		}
	case timestampFieldType:
		timestampValues, ok := f.Values.(*filters.Filter_TimestampValues)
		if !ok {
			return "", nil, fetchers.NewUnsupportedValuesError(f)
		}
		values := timestampValues.TimestampValues.Values
		if len(values) < 1 {
			return "", nil, fetchers.NewMissingValuesError(f)
		}
		switch f.Operation {
		case filters.Filter_OPERATION_GREATER, filters.Filter_OPERATION_LESS:
			if len(values) > 1 {
				return "", nil, fetchers.NewTooManyValuesError(f)
			}
			value, err := ptypes.Timestamp(values[0])
			if err != nil {
				return "", nil, fmt.Errorf("%s: %w", err.Error(), fetchers.FilterError)
			}
			tpl := "AND l.%s %s $%d::timestamp"
			return fmt.Sprintf(tpl, f.Field, opToSQL(f.Operation), paramID), value, nil
		default:
			return "", nil, fetchers.NewUnsupportedOperationError(f)
		}
	default:
		return "", nil, fetchers.NewUnsupportedFieldError(f)
	}
}

func strValuesToSQL(vals []string) string {
	return "{" + strings.Join(vals, ",") + "}"
}

func intValuesToSQL(vals []int32) string {
	var strVals []string
	for _, val := range vals {
		strVals = append(strVals, strconv.Itoa(int(val)))
	}
	return "{" + strings.Join(strVals, ",") + "}"
}

func opToSQL(op filters.Filter_Operation) string {
	switch op {
	case filters.Filter_OPERATION_EQUAL:
		return "="
	case filters.Filter_OPERATION_NOT_EQUAL:
		return "!="
	case filters.Filter_OPERATION_LESS:
		return "<"
	case filters.Filter_OPERATION_GREATER:
		return ">"
	case filters.Filter_OPERATION_IN:
		return "IN"
	case filters.Filter_OPERATION_NOT_IN:
		return "NOT IN"
	default:
		panic(fmt.Sprintf("invalid operation: %s", op))
	}
}
