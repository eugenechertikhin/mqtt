package broker

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/eugenechertikhin/mqtt/db"
	"github.com/eugenechertikhin/mqtt/packet"
)

// startTestBroker spins up a broker on a random loopback port and returns its address.
func startTestBroker(t *testing.T) string {
	t.Helper()

	if err := db.Open(filepath.Join(t.TempDir(), "test.db")); err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	b := NewBroker(false, true) // allow anonymous connections for the test

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go b.newConnection(conn)
		}
	}()

	return ln.Addr().String()
}

// connect performs the CONNECT handshake and returns the live connection.
func connect(t *testing.T, addr, clientID string, clean bool) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	cp := packet.NewConnect()
	cp.Version = 4
	cp.VersionName = "MQTT"
	cp.ClientID = clientID
	cp.CleanSession = clean
	cp.KeepAlive = 0

	if err := packet.WritePacket(conn, cp, false); err != nil {
		conn.Close()
		t.Fatalf("write connect: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	ack, err := packet.ReadPacket(conn, false)
	if err != nil {
		conn.Close()
		t.Fatalf("read connack: %v", err)
	}
	conn.SetReadDeadline(time.Time{})

	ca, ok := ack.(*packet.ConnAckPacket)
	if !ok {
		conn.Close()
		t.Fatalf("expected CONNACK, got %T", ack)
	}
	if ca.ReturnCode != uint8(packet.ConnectAccepted) {
		conn.Close()
		t.Fatalf("connect declined with code %d", ca.ReturnCode)
	}

	return conn
}

// TestPersistentSessionReconnect verifies that when a client reconnects with the
// same client id (persistent session takeover), the old connection is dropped
// but the new connection stays fully functional. This guards against the
// regression where the old connection's failure notification tore down the new
// client.
func TestPersistentSessionReconnect(t *testing.T) {
	addr := startTestBroker(t)

	// first persistent connection
	c1 := connect(t, addr, "dev1", false)
	defer c1.Close()

	// second connection with the same id takes over the session
	c2 := connect(t, addr, "dev1", false)
	defer c2.Close()

	// the old connection must be closed by the broker after the takeover
	c1.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := packet.ReadPacket(c1, false); err == nil {
		t.Fatal("expected the old connection to be closed after takeover")
	}

	// the new connection must remain usable: subscribe ...
	sub := packet.NewSubscribe()
	sub.Id = 10
	sub.Topics = []packet.SubscribePayload{{Topic: "sensors/temp", QoS: packet.AtMostOnce}}
	if err := packet.WritePacket(c2, sub, false); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	c2.SetReadDeadline(time.Now().Add(3 * time.Second))
	suback, err := packet.ReadPacket(c2, false)
	if err != nil {
		t.Fatalf("read suback (new client killed by takeover regression?): %v", err)
	}
	if _, ok := suback.(*packet.SubAckPacket); !ok {
		t.Fatalf("expected SUBACK, got %T", suback)
	}

	// ... and receive a message it publishes to its own subscription
	pub := packet.NewPublish()
	pub.Topic = "sensors/temp"
	pub.Payload = "22.5"
	pub.QoS = packet.AtMostOnce
	if err := packet.WritePacket(c2, pub, false); err != nil {
		t.Fatalf("write publish: %v", err)
	}

	c2.SetReadDeadline(time.Now().Add(3 * time.Second))
	got, err := packet.ReadPacket(c2, false)
	if err != nil {
		t.Fatalf("read delivered publish: %v", err)
	}
	pp, ok := got.(*packet.PublishPacket)
	if !ok {
		t.Fatalf("expected PUBLISH, got %T", got)
	}
	if pp.Topic != "sensors/temp" || pp.Payload != "22.5" {
		t.Fatalf("unexpected delivery: topic=%q payload=%q", pp.Topic, pp.Payload)
	}
}
