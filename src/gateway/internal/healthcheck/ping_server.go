package healthcheck

import (
	"context"
	"net/http"
	"strconv"

	"github.com/maxogod/distro-tp/src/common/logger"
)

type pingServer struct {
	port   int
	server *http.Server
}

func NewPingServer(port int) PingServer {
	return &pingServer{
		port: port,
	}
}

func (p *pingServer) Run() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	p.server = &http.Server{
		Addr:    ":" + strconv.Itoa(p.port),
		Handler: mux,
	}

	logger.Logger.Infof("Starting ping server on port %d", p.port)
	if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Logger.Errorf("Ping server failed: %v", err)
	}
}

func (p *pingServer) Shutdown(ctx context.Context) {
	if p.server != nil {
		if err := p.server.Shutdown(ctx); err != nil {
			logger.Logger.Errorf("Failed to close ping server: %v", err)
		}
	}
}
