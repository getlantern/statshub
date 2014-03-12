// package oauther provides the ability to obtain OAuth tokens and to use
// these inside a Go application.
//
// OAuther is based heavily on this example code:
// https://code.google.com/p/goauth2/source/browse/oauth/example/oauthreq.go
package oauth

import (
	"code.google.com/p/goauth2/oauth"
	"encoding/json"
	"fmt"
	"github.com/oxtoacart/webbrowser"
	"net/http"
	"net/url"
	"sync"
)

type OAuther struct {
	// ClientId: [Required] The clientId for the OAuth app
	ClientId string

	// ClientSecret: [Required] The client secret for the OAuth app
	ClientSecret string

	// TokenURL: [Required] The URL from which to obtain tokens
	TokenURL string

	// Token: [Optional] Previously generated token
	Token *oauth.Token

	// Scope: [Optional] The OAuth scope url(s), separated by space
	Scope string

	// Port: [Optional] The port on which to listen for responses for an OAuth authorization request
	Port string

	// AuthURL: [Optional] The URL
	AuthURL string

	config *oauth.Config

	server *http.Server

	serverMutex sync.Mutex

	codeChannel chan string

	errorChannel chan error
}

// ObtainToken uses an interactive browser session to obtain an oauth.Token
// for the given OAuther.
func (oauther *OAuther) ObtainToken() (err error) {
	oauther.codeChannel = make(chan string)
	oauther.errorChannel = make(chan error)

	oauther.runServerIfNecessary()

	transport := oauther.Transport()

	// Get an authorization code from the data provider.
	// ("Please ask the user if I can access this resource.")
	authCodeUrl, _ := url.QueryUnescape(oauther.config.AuthCodeURL(""))
	webbrowser.Open(authCodeUrl)

	select {
	case authCode := <-oauther.codeChannel:
		// Exchange the authorization code for an access token.
		// ("Here's the code you gave the user, now give me a token!")
		oauther.Token, err = transport.Exchange(authCode)
		return
	case err = <-oauther.errorChannel:
		return
	}
}

// Transport creates a new oauth HTTP transport for the given oauther
func (oauther *OAuther) Transport() *oauth.Transport {
	oauther.config = &oauth.Config{
		AccessType:   "offline",
		ClientId:     oauther.ClientId,
		ClientSecret: oauther.ClientSecret,
		RedirectURL:  fmt.Sprintf("http://localhost:%s/", oauther.Port),
		Scope:        oauther.Scope,
		AuthURL:      oauther.AuthURL,
		TokenURL:     oauther.TokenURL,
	}

	transport := &oauth.Transport{Config: oauther.config}
	transport.Token = oauther.Token
	return transport
}

// ToJSON returns a JSON representation of the OAuther
func (oauther *OAuther) ToJSON() (oautherJson []byte, err error) {
	return json.Marshal(oauther)
}

// FromJSON creates an OAuther from its JSON representation
func FromJSON(oautherJSON []byte) (oauther *OAuther, err error) {
	oauther = &OAuther{}
	json.Unmarshal(oautherJSON, oauther)
	return
}

func (oauther *OAuther) runServerIfNecessary() {
	oauther.serverMutex.Lock()
	defer oauther.serverMutex.Unlock()
	if oauther.server == nil {
		oauther.runServer()
	}
}

func (oauther *OAuther) runServer() {
	handler := http.NewServeMux()
	handler.HandleFunc("/", oauther.handleCallback)
	oauther.server = &http.Server{
		Addr:    fmt.Sprintf(":%s", oauther.Port),
		Handler: handler,
	}
	go func() {
		if err := oauther.server.ListenAndServe(); err != nil {
			oauther.errorChannel <- fmt.Errorf("Unable to start callback server: %s", err)
		}
	}()
}

func (oauther *OAuther) handleCallback(w http.ResponseWriter, r *http.Request) {
	errStrings := r.URL.Query()["error"]
	if len(errStrings) == 1 {
		err := fmt.Errorf("Unable to obtain authorization: %s", errStrings[0])
		oauther.errorChannel <- err
		w.WriteHeader(200)
		w.Write([]byte(fmt.Sprintf("%s", err)))
	} else {
		codes := r.URL.Query()["code"]
		if codes != nil {
			authCode := codes[0]
			oauther.codeChannel <- authCode
			w.WriteHeader(200)
			w.Write([]byte("Authorization received, Thank You!"))
		}
	}
}
