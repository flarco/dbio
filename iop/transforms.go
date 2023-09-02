package iop

import (
	"unicode"

	"github.com/flarco/g"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

type transformFunc func(string) (string, error)

var transforms = map[string]transformFunc{
	"replace_accents": func(val string) (string, error) { return ReplaceAccents(val) },
}

var accentTransformer = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)

func ReplaceAccents(val string) (string, error) {
	newVal, _, err := transform.String(accentTransformer, val)
	if err != nil {
		return val, g.Error(err, "could not transform while running ReplaceAccents")
	}
	return newVal, nil
}
