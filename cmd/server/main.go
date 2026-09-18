package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"food-order-api/internal/domain"
	"food-order-api/internal/handler"
	"food-order-api/internal/middleware"
	"food-order-api/internal/promo"
	"food-order-api/internal/repository"
	"food-order-api/internal/service"
)

const (
	defaultAddress  = ":8080"
	shutdownTimeout = 10 * time.Second
)

// main - starts the HTTP server and waits for shutdown.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	httpHandler, err := newHandler(couponIndexDirectory())
	if err != nil {
		log.Fatalf("initialize server: %v", err)
	}
	server := &http.Server{
		Addr:              defaultAddress,
		Handler:           httpHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("food ordering API listening on %s", defaultAddress)
		serverErrors <- server.ListenAndServe()
	}()
	select {
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	case <-ctx.Done():
		log.Println("shutting down server")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

// newHandler wires repositories, one persisted coupon index version, routes,
// and middleware.
func newHandler(indexDirectory string) (http.Handler, error) {
	productRepo := repository.NewInMemoryProductRepository(seedProducts())
	orderRepo := repository.NewInMemoryOrderRepository()
	couponChecker, loadInfo, err := promo.LoadVersion(indexDirectory)
	if err != nil {
		return nil, err
	}
	log.Printf(
		"coupon index loaded version=%s datasets=%d keys=%d filter_bytes=%d duration=%s",
		loadInfo.Version,
		loadInfo.DatasetCount,
		loadInfo.Keys,
		loadInfo.FilterBytes,
		loadInfo.Duration,
	)
	promoEngine := promo.NewFileEngine(
		couponChecker,
		promo.PercentOff(0.10),
	)
	productHandler := handler.NewProductHandler(service.NewProductService(productRepo))
	orderHandler := handler.NewOrderHandler(
		service.NewOrderService(productRepo, orderRepo, promoEngine),
	)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /product", productHandler.List)
	mux.HandleFunc("GET /product/{productId}", productHandler.Get)
	mux.Handle("POST /order", middleware.APIKey(http.HandlerFunc(orderHandler.Create)))
	return middleware.Logging(
		middleware.RequestID(
			middleware.Recovery(mux),
		),
	), nil
}

// couponIndexDirectory returns the explicit, immutable index version to load.
func couponIndexDirectory() string {
	if directory := os.Getenv("COUPON_INDEX_DIR"); directory != "" {
		return directory
	}
	return filepath.Join("data", "indexes", "v1")
}

// seedProducts - returns the in-memory product catalog used at startup.
func seedProducts() []domain.Product {
	return []domain.Product{
		{
			ID:       "10",
			Name:     "Chicken Waffle",
			Price:    13.30,
			Category: "Waffle",
			Image: domain.Image{
				Thumbnail: "https://orderfoodonline.deno.dev/public/images/image-waffle-thumbnail.jpg",
				Mobile:    "https://orderfoodonline.deno.dev/public/images/image-waffle-mobile.jpg",
				Tablet:    "https://orderfoodonline.deno.dev/public/images/image-waffle-tablet.jpg",
				Desktop:   "https://orderfoodonline.deno.dev/public/images/image-waffle-desktop.jpg",
			},
		},
		{
			ID:       "20",
			Name:     "Chicken Burger",
			Price:    10.00,
			Category: "Burger",
			Image: domain.Image{
				Thumbnail: "https://orderfoodonline.deno.dev/public/images/image-burger-thumbnail.jpg",
				Mobile:    "https://orderfoodonline.deno.dev/public/images/image-burger-mobile.jpg",
				Tablet:    "https://orderfoodonline.deno.dev/public/images/image-burger-tablet.jpg",
				Desktop:   "https://orderfoodonline.deno.dev/public/images/image-burger-desktop.jpg",
			},
		},
		{
			ID:       "30",
			Name:     "Chicken Pizza",
			Price:    15.00,
			Category: "Pizza",
			Image: domain.Image{
				Thumbnail: "https://orderfoodonline.deno.dev/public/images/image-pizza-thumbnail.jpg",
				Mobile:    "https://orderfoodonline.deno.dev/public/images/image-pizza-mobile.jpg",
				Tablet:    "https://orderfoodonline.deno.dev/public/images/image-pizza-tablet.jpg",
				Desktop:   "https://orderfoodonline.deno.dev/public/images/image-pizza-desktop.jpg",
			},
		},
		{
			ID:       "40",
			Name:     "Chicken Wings",
			Price:    12.00,
			Category: "Wings",
			Image: domain.Image{
				Thumbnail: "https://orderfoodonline.deno.dev/public/images/image-wings-thumbnail.jpg",
				Mobile:    "https://orderfoodonline.deno.dev/public/images/image-wings-mobile.jpg",
				Tablet:    "https://orderfoodonline.deno.dev/public/images/image-wings-tablet.jpg",
				Desktop:   "https://orderfoodonline.deno.dev/public/images/image-wings-desktop.jpg",
			},
		},
		{
			ID:       "50",
			Name:     "Chicken Sandwiches",
			Price:    13.00,
			Category: "Sandwiches",
			Image: domain.Image{
				Thumbnail: "https://orderfoodonline.deno.dev/public/images/image-sandwiches-thumbnail.jpg",
				Mobile:    "https://orderfoodonline.deno.dev/public/images/image-sandwiches-mobile.jpg",
				Tablet:    "https://orderfoodonline.deno.dev/public/images/image-sandwiches-tablet.jpg",
				Desktop:   "https://orderfoodonline.deno.dev/public/images/image-sandwiches-desktop.jpg",
			},
		},
	}
}
