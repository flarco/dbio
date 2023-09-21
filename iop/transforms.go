package iop

import (
	"strings"

	"github.com/flarco/g"
	"github.com/google/uuid"
	"golang.org/x/text/transform"
)

type transformFunc func(*StreamProcessor, string) (string, error)

var transforms = map[string]transformFunc{
	"replace_accents": func(sp *StreamProcessor, val string) (string, error) { return ReplaceAccents(sp, val) },
	"trim_space":      func(sp *StreamProcessor, val string) (string, error) { return strings.TrimSpace(val), nil },
	"parse_uuid":      func(sp *StreamProcessor, val string) (string, error) { return ParseUUID(sp, val) },
}

func ReplaceAccents(sp *StreamProcessor, val string) (string, error) {
	newVal, _, err := transform.String(sp.accentTransformer, val)
	if err != nil {
		return val, g.Error(err, "could not transform while running ReplaceAccents")
	}
	return newVal, nil
}

func ParseUUID(sp *StreamProcessor, val string) (string, error) {
	if len(val) == 16 {
		newVal, err := uuid.FromBytes([]byte(val))
		if err != nil {
			return val, g.Error(err, "could not transform while running ParseUUID")
		}
		return newVal.String(), nil
	}
	return val, nil
}
