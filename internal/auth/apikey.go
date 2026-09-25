package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const (
	HeaderAPIKey      = "X-API-Key"
	HeaderWorkerToken = "X-Worker-Token"
)

type Authenticator struct {
	apiKeys      map[string]struct{}
	workerTokens map[string]struct{}
	disabled     bool
}

func New(apiKeys, workerTokens []string, disabled bool) *Authenticator {
	a := &Authenticator{
		apiKeys:      make(map[string]struct{}, len(apiKeys)),
		workerTokens: make(map[string]struct{}, len(workerTokens)),
		disabled:     disabled,
	}
	for _, k := range apiKeys {
		if k != "" {
			a.apiKeys[k] = struct{}{}
		}
	}
	for _, t := range workerTokens {
		if t != "" {
			a.workerTokens[t] = struct{}{}
		}
	}
	return a
}

func (a *Authenticator) ValidAPIKey(key string) bool {
	if a.disabled {
		return true
	}
	_, ok := a.apiKeys[key]
	return ok
}

func (a *Authenticator) ValidWorkerToken(token string) bool {
	if a.disabled {
		return true
	}
	_, ok := a.workerTokens[token]
	return ok
}

func (a *Authenticator) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.disabled {
			next.ServeHTTP(w, r)
			return
		}
		key := r.Header.Get(HeaderAPIKey)
		if key == "" {
			authz := r.Header.Get("Authorization")
			if strings.HasPrefix(strings.ToLower(authz), "apikey ") {
				key = strings.TrimSpace(authz[7:])
			}
		}
		if !a.ValidAPIKey(key) {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func SecureCompare(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
