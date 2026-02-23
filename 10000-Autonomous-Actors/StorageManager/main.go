package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	storagev1 "OlympusGCP-Storage/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/storage/v1"
	"OlympusGCP-Storage/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/storage/v1/storagev1connect"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type StorageServer struct {
	baseDir string
}

func NewStorageServer(baseDir string) *StorageServer {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		slog.Error("Failed to create base storage dir", "error", err)
	}
	return &StorageServer{baseDir: baseDir}
}

func (s *StorageServer) CreateBucket(ctx context.Context, req *connect.Request[storagev1.CreateBucketRequest]) (*connect.Response[storagev1.CreateBucketResponse], error) {
	slog.Info("CreateBucket", "name", req.Msg.Name)
	path := filepath.Join(s.baseDir, req.Msg.Name)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create bucket: %v", err))
	}
	return connect.NewResponse(&storagev1.CreateBucketResponse{}), nil
}

func (s *StorageServer) UploadObject(ctx context.Context, req *connect.Request[storagev1.UploadObjectRequest]) (*connect.Response[storagev1.UploadObjectResponse], error) {
	slog.Info("UploadObject", "bucket", req.Msg.Bucket, "name", req.Msg.Name)
	bucketPath := filepath.Join(s.baseDir, req.Msg.Bucket)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("bucket not found: %s", req.Msg.Bucket))
	}

	objectPath := filepath.Join(bucketPath, req.Msg.Name)
	if err := os.MkdirAll(filepath.Dir(objectPath), 0755); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create object path: %v", err))
	}

	if err := os.WriteFile(objectPath, req.Msg.Data, 0644); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to write object: %v", err))
	}

	return connect.NewResponse(&storagev1.UploadObjectResponse{}), nil
}

func (s *StorageServer) GetDownloadURL(ctx context.Context, req *connect.Request[storagev1.GetDownloadURLRequest]) (*connect.Response[storagev1.GetDownloadURLResponse], error) {
	slog.Info("GetDownloadURL", "bucket", req.Msg.Bucket, "name", req.Msg.Name)
	path := filepath.Join(s.baseDir, req.Msg.Bucket, req.Msg.Name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("object not found: %s/%s", req.Msg.Bucket, req.Msg.Name))
	}

	// Mock URL for local workstation
	url := fmt.Sprintf("file://%s", path)
	return connect.NewResponse(&storagev1.GetDownloadURLResponse{Url: url}), nil
}

func main() {
	storageDir := "../../60000-Information-Storage/StorageData"
	server := NewStorageServer(storageDir)
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(server)
	mux.Handle(path, handler)

	port := "8091" // From genesis.json
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
