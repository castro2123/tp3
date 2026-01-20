package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"

	processingpb "tp3-grpc/pb"

	"google.golang.org/grpc"
)

type hintServer struct {
	processingpb.UnimplementedProcessingHintsServer
}

func (s *hintServer) GetHints(ctx context.Context, req *processingpb.HintsRequest) (*processingpb.HintsResponse, error) {
	chunkSize := getEnvInt("GRPC_CHUNK_SIZE", 200)
	batchSize := getEnvInt("GRPC_BATCH_SIZE", 20)
	batchDelay := getEnvFloat("GRPC_BATCH_DELAY", 0.05)

	note := "hints_from_grpc"
	if req.GetSource() != "" {
		note = "hints_for_" + req.GetSource()
	}

	return &processingpb.HintsResponse{
		ChunkSize:  int32(chunkSize),
		BatchSize:  int32(batchSize),
		BatchDelay: batchDelay,
		Note:       note,
	}, nil
}

func main() {
	host := getEnv("GRPC_HOST", "0.0.0.0")
	port := getEnv("GRPC_PORT", "6000")

	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatalf("[gRPC] failed to listen: %v", err)
	}

	server := grpc.NewServer()
	processingpb.RegisterProcessingHintsServer(server, &hintServer{})
	log.Printf("[gRPC] ProcessingHints on %s:%s", host, port)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("[gRPC] serve failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvFloat(key string, fallback float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
