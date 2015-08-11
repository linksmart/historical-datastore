package cas

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"

	simplexml "github.com/kylewolfe/simplexml"
)

const (
	ticketPath              = "/v1/tickets/"
	oauthProfilePath        = "/oauth2.0/profile"
	casProtocolValidatePath = "/p3/serviceValidate"
)

type CasAuth struct {
	conf Config
}

// Formats error messages
func fErr(err error) error {
	return fmt.Errorf("CAS Error: %s", err.Error())
}

func SetupCasAuth(confPath *string) (*CasAuth, error) {
	var ca CasAuth
	err := ca.loadConfig(confPath)
	if err != nil {
		return nil, fErr(err)
	}

	return &ca, nil
}

// Request Ticker Granting Ticket (TGT) from CAS Server
func (ca *CasAuth) RequestTicketGrantingTicket() (string, error) {
	fmt.Println("CAS: Getting TGT...")
	res, err := http.PostForm(ca.conf.CasServer+ticketPath, url.Values{
		"username": {ca.conf.Username},
		"password": {ca.conf.Password},
	})
	if err != nil {
		return "", fErr(err)
	}
	fmt.Println("CAS:", res.Status)

	// Check for credentials
	if res.StatusCode != http.StatusCreated {
		return "", fErr(fmt.Errorf("Unable to obtain TGT for user `%s`.", ca.conf.Username))
	}

	locationHeader, err := res.Location()
	if err != nil {
		return "", fErr(err)
	}

	return path.Base(locationHeader.Path), nil
}

// Request Service Token from CAS Server
func (ca *CasAuth) RequestServiceToken(TGT, serviceID string) (string, error) {
	fmt.Println("CAS: Getting Service Token...")
	res, err := http.PostForm(ca.conf.CasServer+ticketPath+TGT, url.Values{
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

// Validate Service Token (CAS Protocol)
func (ca *CasAuth) ValidateServiceToken(serviceID, serviceToken string) (bool, map[string]interface{}, error) {
	fmt.Println("CAS: Validating Service Token...")

	bodyMap := make(map[string]interface{})
	res, err := http.Get(fmt.Sprintf("%s%s?service=%s&ticket=%s",
		ca.conf.CasServer, casProtocolValidatePath, serviceID, serviceToken))
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
		bodyMap["error"] = errMsg
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

// Expire the Ticket Granting Ticket
func (ca *CasAuth) Logout(TGT string) error {
	fmt.Println("CAS: Destroying TGT... ")
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s%s", ca.conf.CasServer, ticketPath, TGT), nil)
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
