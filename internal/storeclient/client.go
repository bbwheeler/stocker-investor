package storeclient

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	stockstorev1 "stocker-investor/proto/v1"
)

type Client interface {
	GetStock(ctx context.Context, req *stockstorev1.GetStockRequest) (*stockstorev1.Stock, error)
	GetStocks(ctx context.Context, req *stockstorev1.GetStocksRequest) (*stockstorev1.StockList, error)
}

type client struct {
	stockstorev1.StockStoreClient
	timeout time.Duration
}

func New(addr string, timeout time.Duration) (*client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &client{
		StockStoreClient: stockstorev1.NewStockStoreClient(conn),
		timeout:          timeout,
	}, nil
}

func (c *client) GetStock(ctx context.Context, req *stockstorev1.GetStockRequest) (*stockstorev1.Stock, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.StockStoreClient.GetStock(ctx, req)
}

func (c *client) GetStocks(ctx context.Context, req *stockstorev1.GetStocksRequest) (*stockstorev1.StockList, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.StockStoreClient.GetStocks(ctx, req)
}
