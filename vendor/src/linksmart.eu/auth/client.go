package auth

import "net/http"

type AuthServer struct {
	ServerAddr string
}

// Methods to obtain TGT and Service Ticket (Token)
type TicketObtainer interface {
	RequestTicketGrantingTicket(username, password string) (string, error)
	RequestServiceToken(TGT, serviceID string) (string, error)
	Logout(TGT string) error
}

// Methods to validate Service Ticket (Token)
type TicketValidator interface {
	ValidateServiceToken(serviceID, serviceToken string) (bool, map[string]interface{}, error)
	ValidateServiceTokenHandler(next http.Handler) http.Handler
}
