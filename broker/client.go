package broker

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/MajaSuite/mqtt/packet"
)

type Client struct {
	debug        bool
	conn         net.Conn
	messageId    uint16
	clientId     string
	session      bool                      // persisted session (true) or clean (false)
	keepAlive    uint16                    // keepalive interval (seconds), 0 = disabled
	subscription []packet.SubscribePayload // subscribed topics
	ack          map[string]packet.Packet
	will         *packet.WillMessage
	channel      chan packet.Packet // channel to send message to client over connection
	broker       chan packet.Packet // channel to send message to broker
	done         chan struct{}      // closed on Stop to unblock writers/readers
	stopOnce     sync.Once
}

func NewClient(conn net.Conn, id string, session bool, broker chan packet.Packet, keepAlive uint16, debug bool) *Client {
	return &Client{
		debug:        debug,
		conn:         conn,
		messageId:    1,
		clientId:     id,
		session:      session,
		keepAlive:    keepAlive,
		subscription: []packet.SubscribePayload{},
		ack:          make(map[string]packet.Packet),
		channel:      make(chan packet.Packet, 32),
		broker:       broker,
		done:         make(chan struct{}),
	}
}

func (c *Client) Start() {
	// reader goroutine: read packets from the socket and forward to broker
	go func() {
		for {
			// enforce keepalive: a client must send something within
			// 1.5 * keepalive, otherwise the connection is considered dead
			if c.keepAlive > 0 {
				c.conn.SetReadDeadline(time.Now().Add(time.Duration(c.keepAlive) * time.Second * 3 / 2))
			}

			pkt, err := packet.ReadPacket(c.conn, c.debug)
			if err != nil || pkt == nil {
				c.broker <- &packet.PacketImpl{ClientId: c.clientId}
				log.Printf("%s error read packet, disconnected: %s", c.clientId, err)
				return
			}

			pkt.SetSource(c.clientId)
			c.broker <- pkt

			if pkt.Type() == packet.DISCONNECT {
				return
			}
		}
	}()

	// writer loop: drain the outbound channel to the socket
	for {
		select {
		case p := <-c.channel:
			if c.debug {
				log.Printf("%s message to send %s", c.clientId, p)
			}
			if err := packet.WritePacket(c.conn, p, c.debug); err != nil {
				c.broker <- &packet.PacketImpl{ClientId: c.clientId} // notify broker of unexpected disconnect
				log.Printf("%s disconnect while write to socket %s", c.clientId, err)
				return
			}
		case <-c.done:
			if c.debug {
				log.Printf("client %s stopped", c.clientId)
			}
			return
		}
	}
}

// Send enqueues a packet for delivery to the client. It never panics on a
// stopped client: if the client has been stopped the packet is dropped.
func (c *Client) Send(pkt packet.Packet) {
	select {
	case c.channel <- pkt:
	case <-c.done:
	}
}

func (c *Client) stopped() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func (c *Client) addSubscription(t packet.SubscribePayload) packet.QoS {
	for i, v := range c.subscription {
		if v.Topic == t.Topic {
			c.subscription[i].QoS = t.QoS // upgrade/downgrade existing subscription
			return t.QoS
		}
	}
	c.subscription = append(c.subscription, t)
	return t.QoS
}

func (c *Client) removeSubscription(t packet.SubscribePayload) bool {
	// unsubscribe matches by topic only (QoS is not part of UNSUBSCRIBE)
	for i, v := range c.subscription {
		if v.Topic == t.Topic {
			c.subscription[i] = c.subscription[len(c.subscription)-1]
			c.subscription = c.subscription[:len(c.subscription)-1]
			return true
		}
	}
	return false
}

// Contains reports whether any of the client's subscriptions matches topic.
func (c *Client) Contains(topic string) bool {
	if len(c.subscription) == 0 {
		return false
	}

	t := strings.Split(topic, "/")
	for _, subs := range c.subscription {
		if topicMatch(strings.Split(subs.Topic, "/"), t) {
			return true
		}
	}
	return false
}

// topicMatch implements MQTT topic matching with the '+' (single level) and
// '#' (multi level) wildcards.
func topicMatch(mask, topic []string) bool {
	for i := 0; i < len(mask); i++ {
		if mask[i] == "#" {
			// '#' matches this and every remaining level (including none)
			return true
		}
		if i >= len(topic) {
			return false
		}
		if mask[i] == "+" {
			continue
		}
		if mask[i] != topic[i] {
			return false
		}
	}
	return len(mask) == len(topic)
}

func (c *Client) String() string {
	var will string
	if c.will != nil {
		will = fmt.Sprintf(" will: %s,", c.will.String())
	}

	var subs string
	for _, v := range c.subscription {
		subs += v.String() + ", "
	}

	return fmt.Sprintf("client {clientId: %s, session: %v,%s subscription: [%s]}",
		c.clientId, c.session, will, subs)
}
