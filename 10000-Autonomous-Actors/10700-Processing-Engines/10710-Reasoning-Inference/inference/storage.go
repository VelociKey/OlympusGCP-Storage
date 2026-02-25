package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	storagev1 "OlympusGCP-Storage/gen/v1/storage"
	"connectrpc.com/connect"
	"go.etcd.io/bbolt"
)

type StorageServer struct {
	db      *bbolt.DB
	baseDir string
}

const (
	bucketMetadata = "metadata"
)

func NewStorageServer(storageDir string) *StorageServer {
	os.MkdirAll(storageDir, 0755)
	dbPath := filepath.Join(storageDir, "storage.db")
	
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		slog.Error("Failed to open BoltDB", "path", dbPath, "error", err)
		panic(err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketMetadata))
		return err
	})
	if err != nil { panic(err) }

	return &StorageServer{
		db:      db,
		baseDir: storageDir,
	}
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

	// Save metadata in BoltDB
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketMetadata))
		metaKey := req.Msg.Bucket + "/" + req.Msg.Name
		
		meta := make(map[string]string)
		for k, v := range req.Msg.Metadata { meta[k] = v }
		
		data, _ := json.Marshal(meta)
		return b.Put([]byte(metaKey), data)
	})

	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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
	err = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketMetadata))
		metaKey := req.Msg.Bucket + "/" + req.Msg.Name
		data := b.Get([]byte(metaKey))
		if data != nil {
			return json.Unmarshal(data, &metadata)
		}
		return nil
	})

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
		if err != nil || info.IsDir() { return nil }
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

func (s *StorageServer) Close() error {
	return s.db.Close()
}
