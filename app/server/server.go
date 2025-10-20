package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/cache"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Server struct {
	cache             *cache.Cache
	config            *Config
	errorC            chan error
	listener          net.Listener
	port              int
	role              string
	replicationId     string
	replicationOffset int
	stoppedC          chan struct{}
}

type ServerOpts struct {
	Config    *Config
	IsReplica bool
	Port      int
}

func NewServer(opts ServerOpts) *Server {
	role := "master"

	if opts.IsReplica {
		role = "slave"
	}

	return &Server{
		cache:             cache.NewCache(),
		config:            opts.Config,
		errorC:            make(chan error, 1),
		port:              opts.Port,
		replicationId:     utils.GenerateRandomString(40),
		replicationOffset: 0,
		role:              role,
		stoppedC:          make(chan struct{}, 1),
	}
}

func (s *Server) Start() error {
	// attempt to loadRdb file if present.
	if err := s.loadRdbFile(); err != nil {
		return err
	}

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)

	fmt.Printf("Listening on %s\n", addr)
	s.listener = listener

	go s.startConnectionListener()

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

// When the "dir" and "dbfilename" options are provided
// it parses the Redis Database file and adds the parsed database entries to the
// server's cache.
func (s *Server) loadRdbFile() error {
	src := path.Join(s.config.Get("dir"), s.config.Get("dbfilename"))

	if !utils.FileExists(src) {
		return nil
	}

	parser := rdb.NewParser()

	entries, err := parser.Parse(src)

	if err != nil {
		return fmt.Errorf("failed to load \"%s\" file: %w", src, err)
	}

	for _, entry := range entries {
		// todo: support multiple logical databases
		if entry.DatabaseIndex != 0 {
			continue
		}

		s.cache.SetItem(entry.Key, entry.Value, entry.Expiry)
	}

	return nil
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
