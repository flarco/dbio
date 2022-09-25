package airbyte

import (
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestGithub(t *testing.T) {
	connectors, err := GetSourceConnectors(false)
	g.AssertNoError(t, err)
	assert.Greater(t, len(connectors), 0)
	// g.P(connectors.Names())

	c, err := connectors.Get("GitHub")
	if !g.AssertNoError(t, err) {
		return
	}

	// assert.Equal(t, "Github Source Spec", c.Specification.ConnectionSpecification.Title)

	token := os.Getenv("GITHUB_PAT")
	if token == "" {
		token = os.Getenv("ACCESS_TOKEN")
	}

	config := g.M(
		"credentials", g.M(
			"personal_access_token", token,
		),
		"start_date", "2022-03-01T00:00:00Z",
		"repository", "flarco/dbio",
	)
	s, err := c.Check(config)
	if !g.AssertNoError(t, err) {
		return
	}
	assert.Equal(t, StatusSucceeded, s.Status)
	// g.P(s)

	ac, err := c.Discover(config)
	if !g.AssertNoError(t, err) {
		return
	}
	assert.NotEmpty(t, ac.Streams)
	g.P(ac.Streams.Names())

	catalog := ConfiguredAirbyteCatalog{
		Streams: []ConfiguredAirbyteStream{
			{
				Stream:              ac.GetStream("commits"),
				SyncMode:            SyncModeIncremental,
				DestinationSyncMode: DestinationSyncModeOverwrite,
			},
		},
	}

	ds, err := c.Read(config, catalog, g.M(), nil)
	if g.AssertNoError(t, err) {
		data, err := ds.Collect(0)
		g.AssertNoError(t, err)
		assert.Greater(t, len(data.Columns), 0)
		assert.Greater(t, len(data.Rows), 0)
		g.P(data.Columns.Names())
		g.P(len(data.Rows))
	}
}

func TestPostgres(t *testing.T) {
	connectors, err := GetSourceConnectors(false)
	g.AssertNoError(t, err)
	assert.Greater(t, len(connectors), 0)
	// g.P(connectors.Names())

	c, err := connectors.Get("Postgres")
	if !g.AssertNoError(t, err) {
		return
	}

	// propagate cancel
	go func() {
		sigintChan := make(chan os.Signal, 1)
		signal.Notify(sigintChan, syscall.SIGINT)
		<-sigintChan
		c.ctx.Cancel()
	}()

	config := g.M(
		"host", "host.docker.internal",
		"port", 25432,
		"database", "postgres",
		"username", "postgres",
		"password", "postgres",
	)

	s, err := c.Check(config)
	if !g.AssertNoError(t, err) {
		return
	}
	assert.Equal(t, StatusSucceeded, s.Status)
}
