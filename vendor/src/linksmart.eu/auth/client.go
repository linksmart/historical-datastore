package auth

import "net/http"

// Methods to login, obtain Service Token, and logout
type TicketObtainer interface {
	// Given valid username and password,
	// 	Login must return a Ticket Granting Ticket (TGT).
	Login(username, password string) (string, error)
	// Given valid TGT and serviceID,
	//	RequestServiceToken must return a Service Token.
	RequestServiceToken(TGT, serviceID string) (string, error)
	// Given a valid TGT,
	// 	Logout must expire it.
	Logout(TGT string) error
}

// Methods to validate Service Token
type TicketValidator interface {
	// Given a valid serviceToken for the specified serviceID,
	//	ValidateServiceToken must return true with a set of user attributes.
	ValidateServiceToken(serviceToken string) (bool, map[string]string, error)
	// A HTTP handler wraping ValidateServiceToken
	//	which resonds based on the X_auth_token entity header
	ValidateServiceTokenHandler(next http.Handler) http.Handler
}
