package main

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"mcp-go/mcp"

	"OlympusGCP-Storage/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/storage/v1/storagev1connect"
	storagev1 "OlympusGCP-Storage/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/storage/v1"
	"Olympus2/90000-Enablement-Labs/P0000-pkg/000-mcp-bridge"
)

func main() {
	s := mcpbridge.NewBridgeServer("OlympusStorageBridge", "1.0.0")

	client := storagev1connect.NewStorageServiceClient(
		http.DefaultClient,
		"http://localhost:8091",
	)

	s.AddTool(mcp.NewTool("storage_create_bucket",
		mcp.WithDescription("Create a new storage bucket. Args: {name: string}"),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		m, err := mcpbridge.ExtractMap(request)
		if err != nil {
			return mcpbridge.HandleError(err)
		}

		name, _ := m["name"].(string)

		_, err = client.CreateBucket(ctx, connect.NewRequest(&storagev1.CreateBucketRequest{
			Name: name,
		}))
		if err != nil {
			return mcpbridge.HandleError(err)
		}

		return mcp.NewToolResultText(fmt.Sprintf("Bucket '%s' created successfully.", name)), nil
	})

	s.AddTool(mcp.NewTool("storage_upload",
		mcp.WithDescription("Upload an object to a bucket. Args: {bucket: string, name: string, data: string}"),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		m, err := mcpbridge.ExtractMap(request)
		if err != nil {
			return mcpbridge.HandleError(err)
		}

		bucket, _ := m["bucket"].(string)
		name, _ := m["name"].(string)
		data, _ := m["data"].(string)

		_, err = client.UploadObject(ctx, connect.NewRequest(&storagev1.UploadObjectRequest{
			Bucket: bucket,
			Name:   name,
			Data:   []byte(data),
		}))
		if err != nil {
			return mcpbridge.HandleError(err)
		}

		return mcp.NewToolResultText(fmt.Sprintf("Object '%s' uploaded to bucket '%s'.", name, bucket)), nil
	})

	s.Run()
}
