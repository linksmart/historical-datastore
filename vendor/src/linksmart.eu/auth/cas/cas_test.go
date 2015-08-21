package cas

// Testing successful TGT, Token generation and TGT expiration
// Test executes from project source directory and requires config file.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"testing"
)

type conf struct {
	ServerAddr string `json:"serverAddr"`
	ServiceID  string `json:"serviceID"`
	Username   string `json:"username"`
	Password   string `json:"password"`
}

func TestAuthProcedure(t *testing.T) {
	rulesConfPath := flag.String("rulesconf", "conf_validation.json", "Auth server test config file path")

	// Load test config file
	confPath := flag.String("conf", "conf_obtainer.json", "Auth server test config file path")
	flag.Parse()
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		t.Fatal(err.Error())
	}
	var c conf
	err = json.Unmarshal(file, &c)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Setup ticket obtainer
	o := NewObtainer(c.ServerAddr)

	// Get Ticket Granting Ticket
	TGT, err := o.Login(c.Username, c.Password)
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	// Get Service ticket
	ticket, err := o.RequestTicket(TGT, c.ServiceID)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	fmt.Println("Token", ticket)

	// Setup ticket validator
	v, err := NewValidator(rulesConfPath)
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	// Validate Ticket
	valid, _, err := v.Validate(ticket)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	if !valid {
		t.Fatal("Valid token is flagged as invalid!")
	}

	//	// Expire the Ticket
	//	err = to.Logout(TGT)
	//	if err != nil {
	//		// CAS bug: first DELETE query responds 500
	//		//t.Fatal(err.Error())
	//	}

}
