package saas

import (
	"github.com/flarco/g"
)

// HubspotAPI is for hubspot
// https://developers.hubspot.com/docs/api/overview
type HubspotAPI struct {
	BaseAPI
}

// Init initializes
func (api *HubspotAPI) Init() (err error) {
	api.Provider = Hubspot
	api.BaseURL = "https://api.hubapi.com"
	api.Key = api.GetProp("HUBSPOT_API_KEY")

	if api.Key == "" {
		err = g.Error("did not provide HUBSPOT_API_KEY")
		return
	}

	api.DefHeaders = map[string]string{
		"Content-Type": "application/json",
	}

	api.DefParams = map[string]string{
		"archived": "false",
		"hapikey":  api.Key,
	}

	return api.BaseAPI.Init()
}
