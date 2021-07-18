package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/flarco/dbio/saas/airbyte"
	"github.com/flarco/g"
	"github.com/flarco/g/process"
	"gopkg.in/yaml.v3"
)

func main() {
	syncDefinitions(true)
}

// syncDefinitions will pull specs for all connectors
// and write to the specification folder
func syncDefinitions(force bool) {
	//
	connectors, err := airbyte.GetSourceConnectors(true)
	g.LogFatal(err)

	// run spec on all of them to get the cred properties
	// then clean up
	for _, c := range connectors {
		g.Info("synching " + c.Definition.Name)
		getSpecification(c, force)
	}
}

func getSpecification(c airbyte.Connector, force bool) (err error) {
	wd, _ := os.Getwd()
	filePath := g.F(
		"%s/specifications/%s.yaml",
		filepath.Dir(wd),
		c.Key(),
	)
	if !force && g.PathExists(filePath) {
		return
	}

	p, _ := process.NewProc("docker")

	cleanUp := func() {
		// clean up image
		err := p.Run("image", "rm", c.Definition.Image())
		g.LogError(err, "could not remove image spec for: "+c.Definition.Name)
	}

	// pull image
	err = p.Run("pull", c.Definition.Image())
	g.LogError(err, "could not pull image: "+c.Definition.Image())
	if err != nil {
		time.Sleep(5 * time.Second)
	}

	err = c.GetSpec()
	if err != nil {
		g.LogError(err, "could not get spec for: "+c.Definition.Name)
		return
	}
	defer cleanUp()

	specBytes, err := yaml.Marshal(c.Specification)
	if err != nil {
		g.LogError(err, "could not marshall spec for: "+c.Definition.Name)
		return
	}
	err = ioutil.WriteFile(filePath, specBytes, 0755)
	if err != nil {
		g.LogError(err, "could not write spec for: "+c.Definition.Name)
		return
	}
	return
}
