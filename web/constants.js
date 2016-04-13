// Constants.js

// Attributes of data responses
const DATA_ATTRIBUTES = {
	"name": "n",
	"time": "t",
	"value": "v",
	"unit": "u"
};

// Attributes of aggregation responses
const AGGR_ATTRIBUTES = {
	"name": "n",
	"starttime": "ts",
	"endtime": "te"
};

// Attributes that are hidden by default
const HIDDEN_TABLE_COLUMNS = ["url", "data", "retention", "format", "type"];

// Path to auto-generated config file by HDS
const CONFIG_FILE = "conf/autogen_config.json";

// Datasources to be queried in every request to Registry API
const REG_PER_PAGE = 100;
// Data entries to be queried in every request to Data API
const DATA_PER_PAGE = 1000;
// Aggregated aata entries to be queried in every request to Aggregation API
const AGGR_PER_PAGE = 1000;