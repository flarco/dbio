package airbyte

import (
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestAirbyte(t *testing.T) {
	connectors, err := GetSourceConnectors(false)
	g.AssertNoError(t, err)
	assert.Greater(t, len(connectors), 0)
	// g.P(connectors.Names())

	c, err := connectors.Get("GitHub")
	g.AssertNoError(t, err)
	assert.Equal(t, "Github Source Spec", c.Specification.ConnectionSpecification.Title)

	config := g.M(
		"access_token", os.Getenv("ACCESS_TOKEN"),
		"repository", "flarco/dbio",
	)
	s, err := c.Check(config)
	g.AssertNoError(t, err)
	assert.Equal(t, StatusSucceeded, s.Status)
	// g.P(s)

	ac, err := c.Discover(config)
	g.AssertNoError(t, err)
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

	ds, err := c.Read(config, catalog, g.M())
	if g.AssertNoError(t, err) {
		data, err := ds.Collect(0)
		g.AssertNoError(t, err)
		assert.Greater(t, len(data.Columns), 0)
		assert.Greater(t, len(data.Rows), 0)
		g.P(data.Columns.Names())
		g.P(len(data.Rows))
	}
}
