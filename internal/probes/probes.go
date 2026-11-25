// Package probes serves /v1/livez and /v1/readyz; every other path is 404.
package probes

import "net/http"

// Handler returns the probe handler; ready reports readiness.
func Handler(ready func() bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Path {
		case "/v1/livez":
			w.WriteHeader(http.StatusOK)
		case "/v1/readyz":
			if ready() {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		default:
			http.NotFound(w, r)
		}
	})
}

// Ready reports ready only while the database answers and the broker
// connection is up.
func Ready(ping func() error, brokerConnected func() bool) func() bool {
	return func() bool { return brokerConnected() && ping() == nil }
}
