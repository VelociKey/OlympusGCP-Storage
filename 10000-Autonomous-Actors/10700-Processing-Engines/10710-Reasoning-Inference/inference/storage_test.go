package inference

import (
	"context"
	"testing"

	storagev1 "OlympusGCP-Storage/gen/v1/storage"
	"connectrpc.com/connect"
)

func TestStorageServer_CoverageExpansion(t *testing.T) {
	tempDir := t.TempDir()
	server := NewStorageServer(tempDir)
	defer server.Close()
	ctx := context.Background()

	// 1. Test CreateBucket
	_, err := server.CreateBucket(ctx, connect.NewRequest(&storagev1.CreateBucketRequest{
		Name: "b1",
	}))
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// 2. Test UploadObject
	_, err = server.UploadObject(ctx, connect.NewRequest(&storagev1.UploadObjectRequest{
		Bucket: "b1",
		Name: "o1",
		Data: []byte("content"),
	}))
	if err != nil {
		t.Fatalf("UploadObject failed: %v", err)
	}

	// 3. Test GetObjectMetadata
	res, err := server.GetObjectMetadata(ctx, connect.NewRequest(&storagev1.GetObjectMetadataRequest{
		Bucket: "b1",
		Name: "o1",
	}))
	if err != nil || res.Msg.Name != "o1" {
		t.Error("GetObjectMetadata failed")
	}

	// 4. Test ListObjects
	listRes, err := server.ListObjects(ctx, connect.NewRequest(&storagev1.ListObjectsRequest{
		Bucket: "b1",
	}))
	if err != nil || len(listRes.Msg.ObjectNames) != 1 {
		t.Error("ListObjects failed")
	}

	// 5. Test GetDownloadURL
	_, err = server.GetDownloadURL(ctx, connect.NewRequest(&storagev1.GetDownloadURLRequest{
		Bucket: "b1",
		Name: "o1",
	}))
	if err != nil {
		t.Error("GetDownloadURL failed")
	}
}
