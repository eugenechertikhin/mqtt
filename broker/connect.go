package broker

import (
	"log"
	"net"

	"github.com/eugenechertikhin/mqtt/db"
	"github.com/eugenechertikhin/mqtt/packet"
)

func (b *Broker) newConnection(conn net.Conn) {
	pkt, err := packet.ReadPacket(conn, b.debug)
	if err != nil {
		log.Println("new connection: error read packet", err)
		conn.Close()
		return
	}

	if pkt.Type() != packet.CONNECT {
		log.Println("new connection: wrong packet. expect CONNECT")
		conn.Close()
		return
	}

	connPacket := pkt.(*packet.ConnPacket)
	res := packet.NewConnAck()
	res.ReturnCode = uint8(packet.ConnectAccepted)

	// check version, now only 4 (3.1.1)
	if connPacket.Version != 4 {
		res.ReturnCode = uint8(packet.ConnectUnacceptableProtocol)
	}

	// check authorization
	if res.ReturnCode == uint8(packet.ConnectAccepted) {
		if len(connPacket.Username) > 0 {
			if err := db.CheckAuth(connPacket.Username, connPacket.Password); err != nil {
				log.Printf("new connection: authorisation failed: %s", err)
				res.ReturnCode = uint8(packet.ConnectBadUserPass)
			}
		} else if !b.allowAnonymous {
			log.Println("new connection: anonymous connections are not allowed")
			res.ReturnCode = uint8(packet.ConnectNotAuthorized)
		}
	}

	// client id handling
	if res.ReturnCode == uint8(packet.ConnectAccepted) && len(connPacket.ClientID) == 0 {
		if connPacket.CleanSession {
			// server assigns an id for anonymous clean sessions
			connPacket.ClientID = b.nextAnonID()
		} else {
			// a persistent session requires a client-supplied id
			res.ReturnCode = uint8(packet.ConnectIndentifierRejected)
		}
	}

	// statefull session only when accepted and clean session flag is not set
	res.Session = res.ReturnCode == uint8(packet.ConnectAccepted) && !connPacket.CleanSession

	if err := packet.WritePacket(conn, res, b.debug); err != nil {
		log.Println("new connection: error send response packet", err)
		conn.Close()
		return
	}

	// close connection if not authorized
	if res.ReturnCode != uint8(packet.ConnectAccepted) {
		log.Println("new connection: connection declined")
		conn.Close()
		return
	}

	// register the client
	b.mu.Lock()
	var client *Client
	existing := b.clients[connPacket.ClientID]

	if res.Session {
		client = NewClient(conn, connPacket.ClientID, true, b.channel, connPacket.KeepAlive, b.debug)

		if existing != nil {
			// take over the existing persistent session, preserving its state
			client.subscription = existing.subscription
			client.messageId = existing.messageId
			client.ack = existing.ack
			if connPacket.Will != nil {
				client.will = connPacket.Will
			} else {
				client.will = existing.will
			}
			existing.Stop()
		} else {
			client.will = connPacket.Will
			// restore subscriptions persisted from a previous session
			if subs, err := db.FetchSubcription(connPacket.ClientID); err == nil {
				for topic, qos := range subs {
					client.subscription = append(client.subscription,
						packet.SubscribePayload{Topic: topic, QoS: packet.QoS(qos)})
				}
			}
		}
	} else {
		// clean session: drop any previous state for this id
		if existing != nil {
			existing.Stop()
		}
		client = NewClient(conn, connPacket.ClientID, false, b.channel, connPacket.KeepAlive, b.debug)
		client.will = connPacket.Will
	}

	b.clients[connPacket.ClientID] = client
	b.mu.Unlock()

	// start managing the client (blocks until it disconnects)
	client.Start()
}
