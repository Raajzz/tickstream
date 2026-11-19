package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func authServer(token string) http.Handler {
	a := NewAuth(token)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /login", a.loginPage)
	mux.HandleFunc("POST /login", a.login)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("secret")) })
	return a.Middleware(mux)
}

func do(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAuthBlocksWithoutCredentials(t *testing.T) {
	h := authServer("s3cret")
	if w := do(h, httptest.NewRequest("GET", "/stats", nil)); w.Code != 401 {
		t.Fatalf("api: got %d", w.Code)
	}
	if w := do(h, httptest.NewRequest("GET", "/", nil)); w.Code != 303 || w.Header().Get("Location") != "/login" {
		t.Fatalf("dashboard should redirect to login, got %d", w.Code)
	}
	if w := do(h, httptest.NewRequest("GET", "/healthz", nil)); w.Code != 200 {
		t.Fatalf("healthz must stay open for Fly checks, got %d", w.Code)
	}
}

func TestBearerToken(t *testing.T) {
	h := authServer("s3cret")
	r := httptest.NewRequest("GET", "/stats", nil)
	r.Header.Set("Authorization", "Bearer wrong")
	if do(h, r).Code != 401 {
		t.Fatal("wrong token accepted")
	}
	r.Header.Set("Authorization", "Bearer s3cret")
	if do(h, r).Code != 200 {
		t.Fatal("right token rejected")
	}
}

func TestLoginCookieAndRateLimit(t *testing.T) {
	h := authServer("s3cret")
	post := func(pw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/login", strings.NewReader(url.Values{"password": {pw}}.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return do(h, r)
	}
	w := post("s3cret")
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].Value == "s3cret" {
		t.Fatalf("expected one HttpOnly cookie that isn't the raw token, got %+v", cookies)
	}
	r := httptest.NewRequest("GET", "/stats", nil)
	r.AddCookie(cookies[0])
	if do(h, r).Code != 200 {
		t.Fatal("cookie not accepted")
	}
	for range maxFailsPerIP {
		post("nope")
	}
	if loc := post("s3cret").Header().Get("Location"); loc != "/login?err=rate" {
		t.Fatalf("expected rate limit after %d failures, got %q", maxFailsPerIP, loc)
	}
}

func TestAuthDisabledWhenNoToken(t *testing.T) {
	if do(authServer(""), httptest.NewRequest("GET", "/stats", nil)).Code != 200 {
		t.Fatal("empty token should disable auth")
	}
}
