package cas

import "linksmart.eu/auth"

type TicketObtainerClient struct {
	to        auth.TicketObtainer
	username  string
	password  string
	serviceID string
	tgt       string
}

// Service Ticket (Token) Validator
func NewTicketObtainerClient(serverAddr, username, password, serviceID string) *TicketObtainerClient {
	// Setup ticket obtainer
	to := NewTicketObtainer(serverAddr)
	return &TicketObtainerClient{
		to:        to,
		username:  username,
		password:  password,
		serviceID: serviceID,
	}
}

func (c *TicketObtainerClient) New() (string, error) {
	// Get Ticket Granting Ticket
	TGT, err := c.to.Login(c.username, c.password)
	if err != nil {
		return "", err
	}

	// Get Service Token
	serviceToken, err := c.to.RequestServiceToken(TGT, c.serviceID)
	if err != nil {
		return "", err
	}

	// Keep a copy for renewal references
	c.tgt = TGT

	return serviceToken, nil
}

func (c *TicketObtainerClient) Renew() (string, error) {
	// Renew Service Token using previous TGT
	serviceToken, err := c.to.RequestServiceToken(c.tgt, c.serviceID)
	if err != nil {
		// Get a new Ticket Granting Ticket
		TGT, err := c.to.Login(c.username, c.password)
		if err != nil {
			return "", err
		}
		// Get Service Token
		serviceToken, err := c.to.RequestServiceToken(TGT, c.serviceID)
		if err != nil {
			return "", err
		}
		// Keep a copy for future renewal references
		c.tgt = TGT

		return serviceToken, nil
	}
	return serviceToken, nil
}
