package connection

import (
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestConnection(t *testing.T) {
	m := g.M(
		"url", os.Getenv("POSTGRES_URL"),
	)
	c1, err := NewConnection("POSTGRES", "postgres", m)
	assert.NoError(t, err)
	_ = c1

	m = g.M(
		"username", "postgres",
		"password", "postgres",
		"host", "bionic.larco.us",
		"port", 55432,
		"database", "postgres",
		"sslmode", "disable",
	)
	c2, err := NewConnection("POSTGRES", "postgres", m)
	assert.NoError(t, err)
	_ = c2

	_, err = NewConnection("Db", "someother", m)
	assert.NoError(t, err)

	_, err = NewConnectionFromURL("Db", os.Getenv("POSTGRES_URL"))
	assert.NoError(t, err)
}
