package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"food-order-api/internal/promo"
)

func main() {
	inputDirectory := flag.String("input", "./data", "directory containing couponbase1.gz through couponbase3.gz")
	outputDirectory := flag.String("output", "./data/indexes", "root directory for versioned indexes")
	version := flag.String("version", "", "immutable index version name (required)")
	chunkKeys := flag.Int("chunk-keys", 1_000_000, "maximum hashes held while external sorting")
	flag.Parse()
	if *version == "" {
		fmt.Fprintln(os.Stderr, "coupon-index-builder: --version is required")
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	inputs := []string{
		filepath.Join(*inputDirectory, "couponbase1.gz"),
		filepath.Join(*inputDirectory, "couponbase2.gz"),
		filepath.Join(*inputDirectory, "couponbase3.gz"),
	}
	info, err := promo.BuildVersion(ctx, inputs, *outputDirectory, *version, promo.BuildOptions{
		ChunkKeys: *chunkKeys,
	})
	if err != nil {
		log.Fatalf("build coupon index: %v", err)
	}
	log.Printf(
		"coupon index built version=%s datasets=%d unique_keys=%d input_bytes=%d filter_bytes=%d duration=%s",
		info.Version,
		info.DatasetCount,
		info.UniqueKeys,
		info.InputBytes,
		info.FilterBytes,
		info.Duration,
	)
}
