package registry

// EventListener is implemented by storage modules and connectors which need to react to changes in the registry
type EventListener interface {
	CreateHandler(new DataStream) error
	UpdateHandler(old DataStream, new DataStream) error
	DeleteHandler(old DataStream) error
}

// eventHandler implements sequential fav-out/fan-in of events from registry
type eventHandler []EventListener

func (h eventHandler) created(new DataStream) error {
	for i := range h {
		err := h[i].CreateHandler(new)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h eventHandler) updated(old DataStream, new DataStream) error {
	for i := range h {
		err := h[i].UpdateHandler(old, new)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h eventHandler) deleted(old DataStream) error {
	for i := range h {
		err := h[i].DeleteHandler(old)
		if err != nil {
			return err
		}
	}
	return nil
}
