package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"food-order-api/internal/promo"
)

// TestProductEndpoints - checks list, get, invalid id, and missing product status codes.
func TestProductEndpoints(t *testing.T) {
	server := testHandler(t)
	tests := []struct {
		name   string
		path   string
		status int
	}{
		{name: "list products", path: "/product", status: http.StatusOK},
		{name: "get product", path: "/product/10", status: http.StatusOK},
		{name: "invalid product ID", path: "/product/not-a-number", status: http.StatusBadRequest},
		{name: "missing product", path: "/product/999", status: http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.status, response.Body)
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", response.Header().Get("Content-Type"))
			}
		})
	}
}

// TestCreateOrder - checks API key enforcement and a valid coupon discount.
func TestCreateOrder(t *testing.T) {
	server := testHandler(t)
	t.Run("requires API key", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, "/order", strings.NewReader(`{"items":[]}`))
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
		}
	})
	t.Run("creates order and applies valid coupon", func(t *testing.T) {
		request := httptest.NewRequest(
			http.MethodPost,
			"/order",
			strings.NewReader(`{"couponCode":"HAPPYHRS","items":[{"productId":"10","quantity":2}]}`),
		)
		request.Header.Set("api_key", "apitest")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body)
		}
		var order struct {
			Total     float64 `json:"total"`
			Discounts float64 `json:"discounts"`
		}
		if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if order.Total != 23.94 || order.Discounts != 2.66 {
			t.Fatalf("total = %.2f, discounts = %.2f; want 23.94 and 2.66", order.Total, order.Discounts)
		}
	})
}

// testHandler builds and loads the same persisted index used in production.
func testHandler(t *testing.T) http.Handler {
	t.Helper()
	directory := t.TempDir()
	inputs := []string{
		writeCouponFile(t, directory, "couponbase1.gz", "HAPPYHRS", "SUPER100"),
		writeCouponFile(t, directory, "couponbase2.gz", "HAPPYHRS", "FIFTYOFF"),
		writeCouponFile(t, directory, "couponbase3.gz", "FIFTYOFF"),
	}
	outputRoot := t.TempDir()
	if _, err := promo.BuildVersion(context.Background(), inputs, outputRoot, "test-v1", promo.BuildOptions{ChunkKeys: 2}); err != nil {
		t.Fatalf("BuildVersion() error = %v", err)
	}
	httpHandler, err := newHandler(filepath.Join(outputRoot, "test-v1"))
	if err != nil {
		t.Fatalf("newHandler() error = %v", err)
	}
	return httpHandler
}

// writeCouponFile - writes a gzip coupon dump containing the given codes.
func writeCouponFile(t *testing.T, directory, name string, codes ...string) string {
	t.Helper()
	file, err := os.Create(filepath.Join(directory, name))
	if err != nil {
		t.Fatalf("create coupon file: %v", err)
	}
	writer := gzip.NewWriter(file)
	for _, code := range codes {
		if _, err := writer.Write([]byte(code + "\n")); err != nil {
			t.Fatalf("write coupon file: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close coupon file: %v", err)
	}
	return file.Name()
}
