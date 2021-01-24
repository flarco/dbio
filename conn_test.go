package dbio

import (
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestDataConn(t *testing.T) {
	m := g.M(
		"id", "POSTGRES",
		"type", "postgres",
		"data", g.M(
			"url", os.Getenv("POSTGRES_URL"),
		),
	)
	c, err := NewDataConnFromMap(m)
	assert.NoError(t, err)

	assert.Equal(t, c.ID, m["id"])
	assert.Equal(t, c.Type, m["type"])
	assert.Equal(t, c.URL, os.Getenv("POSTGRES_URL"))

	g.M(
		"id", "POSTGRES",
		"type", "postgres",
		"data", g.M(
			"username", "",
			"password", "",
			"host", "",
		),
	)
}
