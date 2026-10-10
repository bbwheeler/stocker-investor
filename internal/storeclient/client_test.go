package storeclient

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	stockstorev1 "stocker-investor/proto/v1"
)

const bufSize = 1024 * 1024

type fakeStockStoreServer struct {
	stockstorev1.UnimplementedStockStoreServer
}

func (f *fakeStockStoreServer) GetStock(ctx context.Context, req *stockstorev1.GetStockRequest) (*stockstorev1.Stock, error) {
	exchange := ""
	if req.Exchange != nil {
		exchange = *req.Exchange
	}
	return &stockstorev1.Stock{
		Symbol:   req.Symbol,
		Exchange: exchange,
		Scores: []*stockstorev1.ScoreEntry{
			{Category: "test", Value: 1.0},
		},
	}, nil
}

func (f *fakeStockStoreServer) GetStocks(ctx context.Context, req *stockstorev1.GetStocksRequest) (*stockstorev1.StockList, error) {
	exchange := ""
	if req.Exchange != nil {
		exchange = *req.Exchange
	}
	return &stockstorev1.StockList{
		Stocks: []*stockstorev1.Stock{
			{
				Symbol:   "AAPL",
				Exchange: exchange,
				Scores:   []*stockstorev1.ScoreEntry{{Category: "test", Value: 1.0}},
			},
		},
	}, nil
}

func newFakeServer(t *testing.T) (*bufconn.Listener, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	stockstorev1.RegisterStockStoreServer(s, &fakeStockStoreServer{})
	go func() {
		_ = s.Serve(lis)
	}()
	return lis, func() {
		s.Stop()
	}
}

func bufDialer(lis *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}
}

func TestClient_GetStock(t *testing.T) {
	lis, cleanup := newFakeServer(t)
	defer cleanup()

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(bufDialer(lis)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create conn: %v", err)
	}
	defer conn.Close()

	c := &client{
		StockStoreClient: stockstorev1.NewStockStoreClient(conn),
		timeout:          5 * time.Second,
	}

	stock, err := c.GetStock(context.Background(), "AAPL", "NASDAQ")
	if err != nil {
		t.Fatalf("GetStock failed: %v", err)
	}
	if stock.Symbol != "AAPL" {
		t.Errorf("expected AAPL, got %s", stock.Symbol)
	}
	if stock.Exchange != "NASDAQ" {
		t.Errorf("expected NASDAQ, got %s", stock.Exchange)
	}
}

func TestClient_GetStocks(t *testing.T) {
	lis, cleanup := newFakeServer(t)
	defer cleanup()

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(bufDialer(lis)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create conn: %v", err)
	}
	defer conn.Close()

	c := &client{
		StockStoreClient: stockstorev1.NewStockStoreClient(conn),
		timeout:          5 * time.Second,
	}

	resp, err := c.GetStocks(context.Background(), &stockstorev1.GetStocksRequest{Limit: 10})
	if err != nil {
		t.Fatalf("GetStocks failed: %v", err)
	}
	if len(resp.Stocks) != 1 {
		t.Errorf("expected 1 stock, got %d", len(resp.Stocks))
	}
}

func TestClient_Timeout(t *testing.T) {
	// A dialer that blocks until the context deadline forces the RPC to hit
	// the client's per-call timeout instead of failing fast on connection.
	blockingDialer := func(ctx context.Context, s string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	conn, err := grpc.NewClient("passthrough:///blocked", grpc.WithContextDialer(blockingDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create conn: %v", err)
	}
	defer conn.Close()

	c := &client{
		StockStoreClient: stockstorev1.NewStockStoreClient(conn),
		timeout:          100 * time.Millisecond,
	}

	start := time.Now()
	_, err = c.GetStock(context.Background(), "AAPL", "NASDAQ")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("error = %v, want a deadline-exceeded error", err)
	}
	if elapsed > 2*100*time.Millisecond {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}
