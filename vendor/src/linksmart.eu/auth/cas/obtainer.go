package cas

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	auth "linksmart.eu/auth"
)

const (
	ticketPath = "/v1/tickets/"
)

type TicketObtainer struct {
	serverAddr string
}

// Service Ticket (Token) Validator
func NewTicketObtainer(serverAddr string) auth.TicketObtainer {
	return &TicketObtainer{serverAddr}
}

// Request Ticker Granting Ticket (TGT) from CAS Server
func (o *TicketObtainer) Login(username, password string) (string, error) {
	fmt.Println("CAS: Getting TGT...")
	res, err := http.PostForm(o.serverAddr+ticketPath, url.Values{
		"username": {username},
		"password": {password},
	})
	if err != nil {
		return "", fErr(err)
	}
	fmt.Println("CAS:", res.Status)

	// Check for credentials
	if res.StatusCode != http.StatusCreated {
		return "", fErr(fmt.Errorf("Unable to obtain ticket (TGT) for user `%s`.", username))
	}

	locationHeader, err := res.Location()
	if err != nil {
		return "", fErr(err)
	}

	return path.Base(locationHeader.Path), nil
}

// Request Service Token from CAS Server
func (o *TicketObtainer) RequestServiceToken(TGT, serviceID string) (string, error) {
	fmt.Println("CAS: Getting Service Token...")
	res, err := http.PostForm(o.serverAddr+ticketPath+TGT, url.Values{
		"service": {serviceID},
	})
	if err != nil {
		return "", fErr(err)
	}
	fmt.Println("CAS:", res.Status)

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return "", fErr(err)
	}
	res.Body.Close()

	// Check for TGT errors
	if res.StatusCode != http.StatusOK {
		return "", fErr(fmt.Errorf(string(body)))
	}

	return string(body), nil
}

// Expire the Ticket Granting Ticket
func (o *TicketObtainer) Logout(TGT string) error {
	fmt.Println("CAS: Logging out (deleting TGT)...")
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s%s", o.serverAddr, ticketPath, TGT), nil)
	if err != nil {
		return fErr(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fErr(err)
	}
	fmt.Println("CAS:", res.Status)

	// Check for server errors
	if res.StatusCode != http.StatusOK {
		return fErr(fmt.Errorf(res.Status))
	}

	return nil
}
