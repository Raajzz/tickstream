package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"tickstream/web"
)

const (
	cookieName    = "ts_auth"
	cookieMaxAge  = 30 * 24 * time.Hour
	failWindow    = 15 * time.Minute
	maxFailsPerIP = 10
)

// Auth protects every route except /healthz and /login with one shared secret.
// Browsers log in once and get an HttpOnly cookie (which EventSource sends
// automatically); scripts send "Authorization: Bearer <token>".
type Auth struct {
	token     string
	cookieVal string // HMAC of a fixed label: stateless, and rotating the token logs everyone out

	mu    sync.Mutex
	fails map[string][]time.Time
}

func NewAuth(token string) *Auth {
	a := &Auth{token: token, fails: map[string][]time.Time{}}
	if token != "" {
		m := hmac.New(sha256.New, []byte(token))
		m.Write([]byte("tickstream-session-v1"))
		a.cookieVal = hex.EncodeToString(m.Sum(nil))
	}
	return a
}

func (a *Auth) Enabled() bool { return a.token != "" }

func (a *Auth) authorized(r *http.Request) bool {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return equal(strings.TrimPrefix(h, "Bearer "), a.token)
	}
	if c, err := r.Cookie(cookieName); err == nil {
		return equal(c.Value, a.cookieVal)
	}
	return false
}

func equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

func (a *Auth) Middleware(next http.Handler) http.Handler {
	if !a.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/login", "/logout":
			next.ServeHTTP(w, r)
			return
		}
		if a.authorized(r) {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/" {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="tickstream"`)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	})
}

func (a *Auth) loginPage(w http.ResponseWriter, r *http.Request) {
	if !a.Enabled() || a.authorized(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(web.Login)
}

func (a *Auth) login(w http.ResponseWriter, r *http.Request) {
	if !a.Enabled() {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	ip := clientIP(r)
	if a.tooManyFails(ip) {
		http.Redirect(w, r, "/login?err=rate", http.StatusSeeOther)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if !equal(r.PostFormValue("password"), a.token) {
		a.recordFail(ip)
		time.Sleep(500 * time.Millisecond) // slows down guessing
		http.Redirect(w, r, "/login?err=bad", http.StatusSeeOther)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: a.cookieVal, Path: "/",
		MaxAge: int(cookieMaxAge.Seconds()), HttpOnly: true,
		Secure: isHTTPS(r), SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (a *Auth) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true,
		Secure: isHTTPS(r), SameSite: http.SameSiteStrictMode})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *Auth) tooManyFails(ip string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	cutoff := time.Now().Add(-failWindow)
	recent := a.fails[ip][:0]
	for _, t := range a.fails[ip] {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	if len(recent) == 0 {
		delete(a.fails, ip)
	} else {
		a.fails[ip] = recent
	}
	return len(recent) >= maxFailsPerIP
}

func (a *Auth) recordFail(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.fails) > 10000 { // bound memory under a distributed attack
		clear(a.fails)
	}
	a.fails[ip] = append(a.fails[ip], time.Now())
}

// clientIP prefers Fly's edge header; RemoteAddr is Fly's proxy when deployed.
func clientIP(r *http.Request) string {
	if ip := r.Header.Get("Fly-Client-IP"); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func isHTTPS(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}
