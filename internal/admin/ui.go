package admin

import (
	"embed"
	"net/http"
	"strings"
)

//go:embed ui
var uiFS embed.FS

// ui serves the operator interface.
//
// One embedded file, no build step, no CDN. A scheduler UI that needs npm to change a column
// is a UI nobody changes, and one that loads anything from the internet is a UI that stops
// working on the isolated network this is most likely deployed on.
//
// Anything that is not a known asset returns index.html, so a deep link the operator pasted
// into a chat still opens — but only for GET, and never for a path under /api, so a mistyped
// API route gets a 404 rather than a page of HTML the client then fails to parse.
func (a *API) ui() http.Handler {
	index, err := uiFS.ReadFile("ui/index.html")
	if err != nil {
		panic("admin: the UI was not embedded: " + err.Error())
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// The UI performs privileged operations on production data and is not intended to be
		// exposed to the public internet, but these cost nothing and remove the easiest ways
		// to turn a reachable UI into a worse problem.
		h := w.Header()
		h.Set("Content-Type", "text/html; charset=utf-8")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy",
			"default-src 'none'; connect-src 'self'; img-src 'self' data:; "+
				"style-src 'unsafe-inline'; script-src 'unsafe-inline'; form-action 'none'; "+
				"frame-ancestors 'none'; base-uri 'none'")
		h.Set("Cache-Control", "no-store")

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = w.Write(index)
	})
}
