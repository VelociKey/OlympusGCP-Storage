package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

	// Save metadata if provided
	if len(req.Msg.Metadata) > 0 {
		metaPath := objectPath + ".metadata.json"
		metaData, _ := json.Marshal(req.Msg.Metadata)
		os.WriteFile(metaPath, metaData, 0644)
	}

	return connect.NewResponse(&storagev1.UploadObjectResponse{}), nil
}

func (s *StorageServer) GetObjectMetadata(ctx context.Context, req *connect.Request[storagev1.GetObjectMetadataRequest]) (*connect.Response[storagev1.GetObjectMetadataResponse], error) {
	slog.Info("GetObjectMetadata", "bucket", req.Msg.Bucket, "name", req.Msg.Name)
	path := filepath.Join(s.baseDir, req.Msg.Bucket, req.Msg.Name)
	info, err := os.Stat(path)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("object not found: %v", err))
	}

	metadata := make(map[string]string)
	metaPath := path + ".metadata.json"
	if data, err := os.ReadFile(metaPath); err == nil {
		json.Unmarshal(data, &metadata)
	}

	return connect.NewResponse(&storagev1.GetObjectMetadataResponse{
		Bucket:   req.Msg.Bucket,
		Name:     req.Msg.Name,
		Size:     info.Size(),
		Metadata: metadata,
	}), nil
}

func (s *StorageServer) ListObjects(ctx context.Context, req *connect.Request[storagev1.ListObjectsRequest]) (*connect.Response[storagev1.ListObjectsResponse], error) {
	slog.Info("ListObjects", "bucket", req.Msg.Bucket, "prefix", req.Msg.Prefix)
	bucketPath := filepath.Join(s.baseDir, req.Msg.Bucket)
	
	var names []string
	filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".metadata.json") {
			return nil
		}
		
		rel, _ := filepath.Rel(bucketPath, path)
		if req.Msg.Prefix == "" || strings.HasPrefix(rel, req.Msg.Prefix) {
			names = append(names, rel)
		}
		return nil
	})

	return connect.NewResponse(&storagev1.ListObjectsResponse{ObjectNames: names}), nil
}

func (s *StorageServer) GetDownloadURL(ctx context.Context, req *connect.Request[storagev1.GetDownloadURLRequest]) (*connect.Response[storagev1.GetDownloadURLResponse], error) {
	slog.Info("GetDownloadURL", "bucket", req.Msg.Bucket, "name", req.Msg.Name)
	path := filepath.Join(s.baseDir, req.Msg.Bucket, req.Msg.Name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("object not found: %s/%s", req.Msg.Bucket, req.Msg.Name))
	}

	url := fmt.Sprintf("file://%s", path)
	return connect.NewResponse(&storagev1.GetDownloadURLResponse{Url: url}), nil
}

func main() {
	storageDir := "../../60000-Information-Storage/StorageData"
	server := NewStorageServer(storageDir)
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(server)
	mux.Handle(path, handler)

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
