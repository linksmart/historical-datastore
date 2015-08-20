/*
	Package auth provides interfaces to obtain and validate service tickets.
	In addition, a set of methods are provided to load auth rules and check
		whether a service token passes one.
*/
package auth

// Interface methods to login, obtain Service Token, and logout
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
