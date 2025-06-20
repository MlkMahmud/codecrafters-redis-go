package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/cache"
)

type Server struct {
	Cache *cache.Cache

	config   map[string]any
	errorC   chan error
	host     string
	listener net.Listener
	port     int
	stoppedC chan struct{}
}

type ServerConfig struct {
	cache.CacheConfig
	Host string
	Port int
}

func NewServer(cfg ServerConfig) *Server {
	return &Server{
		Cache: cache.NewCache(cache.CacheConfig{HZ: cfg.HZ}),

		config:   map[string]any{},
		errorC:   make(chan error, 1),
		host:     cfg.Host,
		port:     cfg.Port,
		stoppedC: make(chan struct{}, 1),
	}
}

func (s *Server) GetConfigProperty(key string) any {
	value, ok := s.config[key]

	if ok {
		return value
	}

	return nil
}

func (s *Server) SetConfigProperty(key string, value any) {
	s.config[key] = value
}

func (s *Server) Run() error {
	doneC := make(chan os.Signal, 1)
	signal.Notify(doneC, syscall.SIGTERM, syscall.SIGINT)

	host := s.host
	port := s.port

	if host == "" {
		host = "0.0.0.0"
	}

	if port == 0 {
		port = 6379
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}

	fmt.Printf("Listening on %s\n", addr)
	s.listener = listener

	s.startBackgroundTasks()
	defer s.stopBackgroundTasks()

	select {
	case <-doneC:
		return nil
	case err := <-s.errorC:
		return err
	}
}

func (s *Server) handleIncomingConnection(_conn net.Conn) {
	fmt.Print("wubba lubba dub dub")
}

func (s *Server) startBackgroundTasks() {
	go s.startConnectionListener()
	go s.Cache.StartCacheCleaner()
}

func (s *Server) startConnectionListener() {
	if s.listener == nil {
		s.errorC <- fmt.Errorf("server listener has not been initialized")
		return
	}

	for {
		conn, err := s.listener.Accept()

		select {
		case <-s.stoppedC:
			return

		default:
			if errors.Is(err, net.ErrClosed) {
				return
			}

			if err != nil {
				s.errorC <- err
				return
			}

			go s.handleIncomingConnection(conn)
		}
	}
}

func (s *Server) stopBackgroundTasks() {
	close(s.stoppedC)
	s.Cache.StopCacheCleaner()

	if s.listener != nil {
		s.listener.Close()
	}
}
