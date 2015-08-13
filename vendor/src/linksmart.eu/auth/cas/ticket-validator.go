package cas

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	simplexml "github.com/kylewolfe/simplexml"
	auth "linksmart.eu/auth"
)

const (
	oauthProfilePath        = "/oauth2.0/profile"
	casProtocolValidatePath = "/p3/serviceValidate"
)

type TicketValidator struct {
	auth.AuthServer
	serviceID     string
	serverEnabled bool
}

// Service Ticket (Token) Validator
func NewTicketValidator(serverAddr, serviceID string, serverEnabled bool) auth.TicketValidator {
	var v TicketValidator
	v.ServerAddr = serverAddr
	v.serviceID = serviceID
	v.serverEnabled = serverEnabled
	return &v
}

// HTTP Handler for service token validation
func (v *TicketValidator) ValidateServiceTokenHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// Authentication is not enabled
		if !v.serverEnabled {
			next.ServeHTTP(w, r)
			return
		}

		X_auth_token := r.Header.Get("X_auth_token")

		if X_auth_token == "" {
			log.Printf("[%s] %q %s\n", r.Method, r.URL.String(), "X_auth_token not specified.")
			errorResponse(http.StatusUnauthorized, "X_auth_token entity header not specified.", w)
			return
		}

		// Validate Token
		valid, body, err := v.ValidateServiceToken(v.serviceID, X_auth_token)
		if err != nil {
			log.Printf("[%s] %q %s\n", r.Method, r.URL.String(), "Auth. server error: "+err.Error())
			errorResponse(http.StatusInternalServerError, "Authorization server error: "+err.Error(), w)
			return
		}
		if !valid {
			if _, ok := body["error"]; ok {
				log.Printf("[%s] %q %s\n", r.Method, r.URL.String(), body["error"])
				errorResponse(http.StatusUnauthorized, "Unauthorized request: "+body["error"].(string), w)
				return
			}
			errorResponse(http.StatusUnauthorized, "Unauthorized request.", w)
			return
		}

		// Valid token, proceed to next handler
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// Validate Service Token (CAS Protocol)
func (v *TicketValidator) ValidateServiceToken(serviceID, serviceToken string) (bool, map[string]interface{}, error) {
	fmt.Println("CAS: Validating Service Token...")

	bodyMap := make(map[string]interface{})
	res, err := http.Get(fmt.Sprintf("%s%s?service=%s&ticket=%s",
		v.ServerAddr, casProtocolValidatePath, serviceID, serviceToken))
	if err != nil {
		return false, bodyMap, fErr(err)
	}
	fmt.Println("CAS:", res.Status)

	// Check for server errors
	if res.StatusCode != http.StatusOK {
		return false, bodyMap, fErr(fmt.Errorf(res.Status))
	}

	// User attributes / error message
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return false, bodyMap, fErr(err)
	}
	res.Body.Close()

	// Create an xml document from response body
	doc, err := simplexml.NewDocumentFromReader(strings.NewReader(string(body)))
	if err != nil {
		return false, bodyMap, fErr(fmt.Errorf("Unexpected error while validating service token."))
	}

	// StatusCode is 200 for all responses (valid, expired, missing)
	// Check if response contains authenticationSuccess tag
	success := doc.Root().Search().ByName("authenticationSuccess").One()
	// There is no authenticationSuccess tag
	if success == nil {
		// Check if response contains authenticationFailure tag
		failure := doc.Root().Search().ByName("authenticationFailure").One()
		if failure == nil {
			return false, bodyMap, fErr(fmt.Errorf("Unexpected error while validating service token."))
		}
		// Extract the error message
		errMsg, err := failure.Value()
		if err != nil {
			return false, bodyMap, fErr(fmt.Errorf("Unexpected error. No error message."))
		}
		bodyMap["error"] = strings.TrimSpace(errMsg)
		return false, bodyMap, nil
	}
	// Token is valid
	fmt.Println("CAS: Token was valid.")
	// Extract username
	userTag := doc.Root().Search().ByName("authenticationSuccess").ByName("user").One()
	if userTag != nil {
		user, err := userTag.Value()
		if err == nil {
			bodyMap["username"] = user
		}
	}
	// Valid token + attributes
	return true, bodyMap, nil
}

// Validate Service Token (OAUTH)
//func (ca *CasAuth) ValidateServiceToken(serviceToken string) (bool, map[string]interface{}, error) {
//	fmt.Println("CAS: Validating Service Token...")

//	var bodyMap map[string]interface{}
//	res, err := http.Get(fmt.Sprintf("%s%s?access_token=%s", ca.conf.CasServer, oauthProfilePath, serviceToken))
//	if err != nil {
//		return false, bodyMap, fErr(err)
//	}
//	fmt.Println("CAS:", res.Status)

//	// Check for server errors
//	if res.StatusCode != http.StatusOK {
//		return false, bodyMap, fErr(fmt.Errorf(res.Status))
//	}

//	// User attributes / error message
//	body, err := ioutil.ReadAll(res.Body)
//	defer res.Body.Close()
//	if err != nil {
//		return false, bodyMap, fErr(err)
//	}
//	res.Body.Close()

//	if len(body) == 0 { // body is empty due to CAS bug
//		fmt.Println("CAS: Token was valid.")
//		return true, bodyMap, nil
//	}

//	err = json.Unmarshal(body, &bodyMap)
//	if err != nil {
//		return false, bodyMap, fErr(err)
//	}
//	// StatusCode is 200 for all responses (valid, expired, missing)
//	// Check the error message
//	errMsg, ok := bodyMap["error"]
//	if ok {
//		fmt.Println("CAS: Error:", errMsg)
//		return false, bodyMap, nil
//	} else {
//		fmt.Println("CAS: Token was valid.")
//	}

//	// Valid token + attributes
//	return true, bodyMap, nil
//}
