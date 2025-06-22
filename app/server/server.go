package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/cache"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Server struct {
	cache    *cache.Cache
	config   map[string]string
	errorC   chan error
	host     string
	hz       int
	listener net.Listener
	port     int
	stoppedC chan struct{}
}

type ServerConfig struct {
	Host string
	// Controls the frequency (in milliseconds) at which the cache performs background tasks like expiring keys.
	HZ   int
	Port int
}

func NewServer(cfg ServerConfig) *Server {
	return &Server{
		cache:    cache.NewCache(),
		config:   map[string]string{},
		errorC:   make(chan error, 1),
		host:     cfg.Host,
		hz:       cfg.HZ,
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

func (s *Server) SetConfigProperty(key string, value string) {
	s.config[key] = value
}

func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)

	fmt.Printf("Listening on %s\n", addr)
	s.listener = listener

	go s.startConnectionListener()
	go s.startCacheCleaner()

	defer s.stop()

	select {
	case <-sigC:
		return nil
	case err := <-s.errorC:
		return err
	}
}

func (s *Server) handleIncomingConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		data, err := resp.Parse(reader)

		select {
		case <-s.stoppedC:
			return

		default:
			if errors.Is(err, io.EOF) {
				return
			}

			if errors.Is(err, resp.ErrSyntax) {
				conn.Write(utils.GenerateErrorString("ERR", err.Error()))
				return
			}

			if err != nil {
				conn.Write(utils.GenerateErrorString("ERR", "unexpected server error"))
				return
			}

			responses := s.handleCommands(data)

			for response := range responses {
				select {
				case <-s.stoppedC:
					return

				default:
					if _, err := conn.Write(response); err != nil {
						return
					}
				}
			}
		}
	}
}

func (s *Server) startCacheCleaner() {
	hz := s.hz

	if hz == 0 {
		hz = 5000
	}

	duration := time.Duration(hz) * time.Millisecond
	timer := time.NewTicker(duration)

	for {
		select {
		case <-s.stoppedC:
			return

		case <-timer.C:
			for key, item := range s.cache.GetItems() {
				select {
				case <-s.stoppedC:
					return

				default:
					expiry := item.GetTTL()
					if expiry.IsZero() {
						continue
					}

					if expiry.Before(time.Now()) {
						s.cache.RemoveItem(key)
					}
				}
			}
		}
	}
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

func (s *Server) stop() {
	close(s.stoppedC)

	if s.listener != nil {
		s.listener.Close()
	}
}
