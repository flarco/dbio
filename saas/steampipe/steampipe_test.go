package steampipe

import (
	"context"
	"testing"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/turbot/steampipe-plugin-github/github"
)

func TestGithub(t *testing.T) {
	plugin := github.Plugin(context.Background())

	tableReqFields := map[string][]string{}
	uniqueReqFields := map[string]int{}
	for key, table := range plugin.TableMap {
		reqFields := []string{}
		if list := table.List; list != nil {
			for _, col := range list.KeyColumns {
				if col.Require == "required" {
					reqFields = append(reqFields, col.Name)
					uniqueReqFields[col.Name] = uniqueReqFields[col.Name] + 1
				}
			}
		}
		tableReqFields[key] = reqFields
	}
	g.PP(lo.Keys(github.ConfigSchema))
	g.PP(uniqueReqFields)
	g.PP(tableReqFields)
}
