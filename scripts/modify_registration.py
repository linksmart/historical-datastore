# A sample script that reads the registry and modifies the mqtt url in all entries

import urllib2
import json

endpoint = "http://iot-linksmart.iot:8085"

contents = urllib2.urlopen(endpoint+"/registry").read()
parsed = json.loads(contents)
for e in parsed["entries"]:
    print e["id"]

    # Modify things
    e["connector"]["mqtt"]["url"] = "ssl://linksmart-dev.fit.fraunhofer.de:8883"

    # PUT
    url = endpoint+"/registry/"+e["id"]
    payload = json.dumps(e)
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data=payload)
    request.get_method = lambda: 'PUT'
    opener.open(request)


