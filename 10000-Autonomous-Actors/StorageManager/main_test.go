package main

import (
	"context"
	"testing"

	storagev1 "OlympusGCP-Storage/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/storage/v1"
	"connectrpc.com/connect"
)

func TestStorageServerAdvanced(t *testing.T) {
	tempDir := t.TempDir()
	server := NewStorageServer(tempDir)
	ctx := context.Background()

	bucket := "meta-bucket"
	server.CreateBucket(ctx, connect.NewRequest(&storagev1.CreateBucketRequest{Name: bucket}))

	// Test Upload with Metadata
	object := "data.bin"
	metadata := map[string]string{"type": "binary", "owner": "jules"}
	_, err := server.UploadObject(ctx, connect.NewRequest(&storagev1.UploadObjectRequest{
		Bucket:   bucket,
		Name:     object,
		Data:     []byte{0, 1, 2},
		Metadata: metadata,
	}))
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// Test GetMetadata
	metaRes, err := server.GetObjectMetadata(ctx, connect.NewRequest(&storagev1.GetObjectMetadataRequest{
		Bucket: bucket,
		Name:   object,
	}))
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}
	if metaRes.Msg.Metadata["type"] != "binary" {
		t.Errorf("Expected metadata type 'binary', got '%s'", metaRes.Msg.Metadata["type"])
	}

	// Test List
	listRes, err := server.ListObjects(ctx, connect.NewRequest(&storagev1.ListObjectsRequest{
		Bucket: bucket,
	}))
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(listRes.Msg.ObjectNames) != 1 {
		t.Errorf("Expected 1 object, got %d", len(listRes.Msg.ObjectNames))
	}
}
