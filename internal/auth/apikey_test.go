package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ghrushneshr25/chronos-dev/internal/auth"
)

func TestValidAPIKey(t *testing.T) {
	a := auth.New([]string{"key-a", "key-b"}, []string{"wtoken"}, false)

	if !a.ValidAPIKey("key-a") {
		t.Fatal("expected key-a valid")
	}
	if a.ValidAPIKey("nope") {
		t.Fatal("expected invalid key rejected")
	}
	if !a.ValidWorkerToken("wtoken") {
		t.Fatal("expected worker token valid")
	}
	if a.ValidWorkerToken("bad") {
		t.Fatal("expected bad worker token rejected")
	}
}

func TestAuthDisabled(t *testing.T) {
	a := auth.New(nil, nil, true)
	if !a.ValidAPIKey("") {
		t.Fatal("disabled auth should accept empty key")
	}
}

func TestHTTPMiddleware(t *testing.T) {
	a := auth.New([]string{"secret"}, nil, false)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := a.HTTPMiddleware(next)

	t.Run("missing key", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("got %d want 401", rr.Code)
		}
	})

	t.Run("header key", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		req.Header.Set(auth.HeaderAPIKey, "secret")
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("got %d want 200", rr.Code)
		}
	})

	t.Run("authorization apikey", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		req.Header.Set("Authorization", "ApiKey secret")
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("got %d want 200", rr.Code)
		}
	})
}

func TestSecureCompare(t *testing.T) {
	if !auth.SecureCompare("abc", "abc") {
		t.Fatal("equal strings")
	}
	if auth.SecureCompare("abc", "abd") {
		t.Fatal("unequal strings")
	}
	if auth.SecureCompare("abc", "ab") {
		t.Fatal("different lengths")
	}
}
