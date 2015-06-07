Historical Datastore
===================

Implementation of the [Historical Datastore Service](https://linksmart.eu/redmine/projects/historical-datastore)


## Dependencies

 * Alice (`github.com/justinas/alice`) for middleware (http.Handler chaining)
 * Gorilla Context (`github.com/gorilla/context`) for sharing data between middlewares
 * Gorilla Mux (`github.com/gorilla/mux`) for pattern-based request routing

## Deployment

# Sample config file:
historical-datastore.json
{
    "http": {
        "bindAddr": "0.0.0.0",
        "bindPort": 8085
    },
    "registry": {},
    "data": {
        "backend": {
            "type": "influxdb",
            "dsn": "http://user:password@localhost:8086/db"
        }
    },
    "aggregation": {}
}
