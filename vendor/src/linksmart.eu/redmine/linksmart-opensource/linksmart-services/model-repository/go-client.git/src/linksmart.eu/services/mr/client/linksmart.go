package client

// LinkSmart® models
const (
	ProtoREST = "REST"
	ProtoMQTT = "MQTT"
)

type Device struct {
	ID          string
	Name        string
	Description string
	Meta        map[string]interface{}
	TTL         int
	Resources   []Resource
}

type Resource struct {
	ID             string
	Name           string
	Meta           map[string]interface{}
	ExtProtocol    interface{}
	IntProtocol    interface{}
	Representation map[string]interface{}
}

type Protocol struct {
	Type         string
	Endpoint     map[string]interface{}
	Methods      []string
	ContentTypes []string
}

// some known protocols for convenience (use type assertion)
type RESTProtocol struct {
	*Protocol
	Endpoint RESTEndpoint
}

type RESTEndpoint struct {
	URL string
}

type MQTTProtocol struct {
	*Protocol
	Endpoint MQTTEndpoint
}

type MQTTEndpoint struct {
	URL      string
	PubTopic string
	SubTopic string
}
