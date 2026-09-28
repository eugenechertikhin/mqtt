package broker

import (
	"crypto/tls"
	"log"
	"net"
	"os"
)

type listener struct {
	debug    bool
	listener net.Listener
	broker   *Broker
}

// fileExists reports whether path points to an existing regular file.
func fileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func NewListener(debug bool, key string, cert string) *listener {
	var l net.Listener
	var err error

	// use TLS only when both certificate and key files are present;
	// otherwise fall back to plaintext
	if fileExists(cert) && fileExists(key) {
		certificate, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			// files exist but are unusable - fail loudly instead of a
			// silent downgrade to plaintext
			log.Println("error load tls certificate/key:", err)
			return nil
		}

		config := &tls.Config{Certificates: []tls.Certificate{certificate}}
		l, err = tls.Listen("tcp", "0.0.0.0:8883", config)
		if err != nil {
			log.Println("error start tls listener:", err)
			return nil
		}
		log.Println("listen (tls) on address", l.Addr())
	} else {
		l, err = net.Listen("tcp", "0.0.0.0:1883")
		if err != nil {
			return nil
		}
		log.Println("listen (plaintext) on address", l.Addr())
	}

	return &listener{debug: debug, listener: l, broker: NewBroker(debug)}
}

func (s *listener) Manage() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Println("error accept connection", err)
			conn.Close()
			continue
		}

		go s.broker.newConnection(conn)
	}
}

func (s *listener) Accept() (net.Conn, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}

	if s.debug {
		log.Println("accept new connection from", conn.RemoteAddr())
	}

	return conn, nil
}

func (s *listener) Close() error {
	log.Println("close listener")
	return s.listener.Close()
}

func (s *listener) Addr() net.Addr {
	return s.listener.Addr()
}
