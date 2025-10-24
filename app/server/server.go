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
	"strings"
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
	Config *Config
	Port   int
}

func NewServer(opts ServerOpts) *Server {
	role := "master"

	if opts.Config.Get("replicaof") != "" {
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

	// attempt to connect to the master server if the server is a replica
	if err := s.connectToMaster(); err != nil {
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

func (s *Server) connectToMaster() error {
	if s.role == "master" {
		return nil
	}

	replicaOf := s.config.Get("replicaof")

	if replicaOf == "" {
		return fmt.Errorf("replicaof option is not provided")
	}

	address := strings.Split(replicaOf, " ")
	host := address[0]
	port := address[1]

	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))

	if err != nil {
		return fmt.Errorf("failed to connect to master server: %w", err)
	}

	defer conn.Close()

	pingCmd := utils.GenerateArrayString([][]byte{utils.GenerateBulkString("PING")})
	// generic success response contains 5 bytes +OK\r\n
	okResponseBuf := make([]byte, 5)
	// PING response contains 7 bytes +PONG\r\n
	pingResponseBuf := make([]byte, 7)

	if _, err := conn.Write(pingCmd); err != nil {
		return fmt.Errorf("failed to PING master server: %w", err)
	}

	if _, err := io.ReadAtLeast(conn, pingResponseBuf, len(pingResponseBuf)); err != nil {
		return fmt.Errorf("failed to received correct PING response from server: %w", err)
	}

	for _, cmd := range [][]byte{
		utils.GenerateArrayString([][]byte{utils.GenerateBulkString("REPLCONF"), utils.GenerateBulkString("listening-port"), utils.GenerateBulkString(fmt.Sprintf("%d", s.port))}),
		utils.GenerateArrayString([][]byte{utils.GenerateBulkString("REPLCONF"), utils.GenerateBulkString("capa"), utils.GenerateBulkString("psync2")})} {

		if _, err := conn.Write(cmd); err != nil {
			return fmt.Errorf("failed to send \"%s\" command: %w", cmd, err)
		}

		if _, err := io.ReadAtLeast(conn, okResponseBuf, len(okResponseBuf)); err != nil {
			return fmt.Errorf("failed to received \"%s\" response from master server: %w", cmd, err)
		}
	}

	return nil
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
