package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func main() {
	// Create API v1 reverse proxy
	urlv1, err := url.Parse("http://forecast_arima:8080")
	if err != nil {
		log.Fatalf("Can't parse API v1 URL: %s\n", err.Error())
	}
	proxyv1 := httputil.NewSingleHostReverseProxy(urlv1)

	// Create API v2 reverse proxy
	urlv2, err := url.Parse("http://forecast_autoreg:8080")
	if err != nil {
		log.Fatalf("Can't parse API v2 URL: %s\n", err.Error())
	}
	proxyv2 := httputil.NewSingleHostReverseProxy(urlv2)

	// Route handlers
	http.HandleFunc("/v1/", func(rw http.ResponseWriter, r *http.Request) {
		proxyv1.ServeHTTP(rw, r)
	})
	http.HandleFunc("/v2/", func(rw http.ResponseWriter, r *http.Request) {
		proxyv2.ServeHTTP(rw, r)
	})

	// Start listening for requests
	log.Fatal(http.ListenAndServe(":8080", nil))
}
