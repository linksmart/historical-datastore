package registry

// DataStreamList describes a registry of registered Data streams
type DataStreamList struct {
	// BrokerURL is the BrokerURL of the DataStreamList API
	URL string `json:"url"`
	// Entries is an array of Data streams
	Streams []DataStream `json:"streams"`
	// Page is the current page in Entries pagination
	Page int `json:"page"`
	// MaxEntries is the results per page in Entries pagination
	PerPage int `json:"per_page"`
	// Total is the total #of pages in Entries pagination
	Total int `json:"total"`
}
