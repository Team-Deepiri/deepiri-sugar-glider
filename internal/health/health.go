package health

import (
	"context"
	"errors"
	"time"
)

type ReadyProbe func(ctx context.Context) error

func CheckReady(timeout time.Duration, probe ReadyProbe) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := probe(ctx); err != nil {
		return errors.New("not ready")
	}
	return nil
}
