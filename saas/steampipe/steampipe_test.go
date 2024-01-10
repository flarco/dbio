package steampipe

import (
	"io/ioutil"
	"testing"

	"github.com/flarco/g"
	"github.com/hashicorp/hcl"
	"github.com/stretchr/testify/assert"
)

// func TestGithub(t *testing.T) {
// 	plugin := github.Plugin(context.Background())

// 	tableReqFields := map[string][]string{}
// 	uniqueReqFields := map[string]int{}
// 	for key, table := range plugin.TableMap {
// 		reqFields := []string{}
// 		if list := table.List; list != nil {
// 			for _, col := range list.KeyColumns {
// 				if col.Require == "required" {
// 					reqFields = append(reqFields, col.Name)
// 					uniqueReqFields[col.Name] = uniqueReqFields[col.Name] + 1
// 				}
// 			}
// 		}
// 		tableReqFields[key] = reqFields
// 	}
// 	g.PP(lo.Keys(github.ConfigSchema))
// 	g.PP(uniqueReqFields)
// 	g.PP(tableReqFields)
// }

func TestCredentials(t *testing.T) {
	credPath := "/Users/fritz/.steampipe/config/github.spc"
	bytes, _ := ioutil.ReadFile(credPath)
	m := g.M()
	err := hcl.Decode(&m, string(bytes))
	assert.NoError(t, err)
	g.Info(g.Pretty(m))
}
