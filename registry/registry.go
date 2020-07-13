package registry

// TimeSeriesList describes a registry of registered time series
type TimeSeriesList struct {
	// BrokerURL is the BrokerURL of the Registry API
	URL string `json:"url"`
	// Entries is an array of time series
	Series []TimeSeries `json:"streams"`
	// Page is the current page in Entries pagination
	Page int `json:"page"`
	// MaxEntries is the results per page in Entries pagination
	PerPage int `json:"per_page"`
	// Total is the total #of pages in Entries pagination
	Total int `json:"total"`
}
