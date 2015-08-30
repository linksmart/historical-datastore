package cas

import (
	"fmt"

	"linksmart.eu/auth"
)

type ObtainerClient struct {
	obtainer  auth.Obtainer
	username  string
	password  string
	serviceID string
	tgt       string
	ticket    string
}

// Service Ticket (Token) Validator
func NewObtainerClient(serverAddr, username, password, serviceID string) *ObtainerClient {
	return &ObtainerClient{
		obtainer:  NewObtainer(serverAddr), // Setup ticket obtainer
		username:  username,
		password:  password,
		serviceID: serviceID,
	}
}

func (c *ObtainerClient) Ticket() string {
	return c.ticket
}

func (c *ObtainerClient) New() (string, error) {
	// Get Ticket Granting Ticket
	TGT, err := c.obtainer.Login(c.username, c.password)
	if err != nil {
		return "", err
	}
	fmt.Println("CAS: First TGT:", TGT)

	// Get Service Ticket
	ticket, err := c.obtainer.RequestTicket(TGT, c.serviceID)
	if err != nil {
		return "", err
	}
	c.ticket = ticket

	// Keep a copy for renewal references
	c.tgt = TGT

	return ticket, nil
}

func (c *ObtainerClient) Renew() (string, error) {
	// Renew Service Ticket using previous TGT
	ticket, err := c.obtainer.RequestTicket(c.tgt, c.serviceID)
	fmt.Println("CAS: New serviceToken:", ticket)
	if err != nil {
		// Get a new Ticket Granting Ticket
		TGT, err := c.obtainer.Login(c.username, c.password)
		if err != nil {
			return "", err
		}
		fmt.Println("CAS: New TGT:", TGT)
		// Get Service Ticket
		ticket, err := c.obtainer.RequestTicket(TGT, c.serviceID)
		if err != nil {
			return "", err
		}
		c.ticket = ticket
		// Keep a copy for future renewal references
		c.tgt = TGT

		return ticket, nil
	}
	return ticket, nil
}

func (c *ObtainerClient) Delete() error {
	err := c.obtainer.Logout(c.tgt)
	if err != nil {
		fmt.Println("CAS:", err.Error())
		return err
	}
	fmt.Println("CAS: TGT was deleted.")
	return nil
}
