package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"OlympusGCP-Storage/gen/v1/storage/storagev1connect"
	"OlympusGCP-Storage/10000-Autonomous-Actors/10700-Processing-Engines/10710-Reasoning-Inference/inference"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	storageDir := "../../60000-Information-Storage/StorageData"
	server := inference.NewStorageServer(storageDir)
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(server)
	mux.Handle(path, handler)

	// Health Check / Pulse
	mux.HandleFunc("/pulse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"HEALTHY", "workspace":"OlympusGCP-Storage", "time":"%s"}`, time.Now().Format(time.RFC3339))
	})

	port := "8091"
	slog.Info("StorageManager starting", "port", port)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 3 * time.Second,
	}
	err := srv.ListenAndServe()
	if err != nil {
		slog.Error("Server failed", "error", err)
	}
}
