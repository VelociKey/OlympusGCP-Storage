package main

import (
	"context"
	"dagger/olympusgcp-storage/internal/dagger"
)

type OlympusGCPStorage struct{}

func (m *OlympusGCPStorage) HelloWorld(ctx context.Context) string {
	return "Hello from OlympusGCP-Storage!"
}

func main() {
	dagger.Serve()
}
