package iop

import (
	"strings"
	"unicode"

	"github.com/flarco/g"
	"github.com/google/uuid"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

type transformFunc func(string) (string, error)

var transforms = map[string]transformFunc{
	"replace_accents": func(val string) (string, error) { return ReplaceAccents(val) },
	"trim_space":      func(val string) (string, error) { return strings.TrimSpace(val), nil },
	"parse_uuid":      func(val string) (string, error) { return ParseUUID(val) },
}

var accentTransformer = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)

func ReplaceAccents(val string) (string, error) {
	newVal, _, err := transform.String(accentTransformer, val)
	if err != nil {
		return val, g.Error(err, "could not transform while running ReplaceAccents")
	}
	return newVal, nil
}

func ParseUUID(val string) (string, error) {
	if len(val) == 16 {
		newVal, err := uuid.FromBytes([]byte(val))
		if err != nil {
			return val, g.Error(err, "could not transform while running ParseUUID")
		}
		return newVal.String(), nil
	}
	return val, nil
}
