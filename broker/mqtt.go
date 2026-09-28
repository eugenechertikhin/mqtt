package broker

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eugenechertikhin/mqtt/db"
	"github.com/eugenechertikhin/mqtt/packet"
)

type Broker struct {
	debug          bool
	allowAnonymous bool
	channel        chan packet.Packet // channel to mqtt broker engine (to push packet, received from client)
	mu             sync.RWMutex       // guards clients and per-client shared state
	clients        map[string]*Client // hashmap of all connected clients
	anonID         uint64             // counter for generated client ids
}

func NewBroker(debug bool, allowAnonymous bool) *Broker {
	broker := &Broker{
		debug:          debug,
		allowAnonymous: allowAnonymous,
		channel:        make(chan packet.Packet),
		clients:        make(map[string]*Client),
	}

	go broker.broker()
	go broker.rescan()

	return broker
}

// nextAnonID returns a unique generated client id for anonymous clean sessions.
func (b *Broker) nextAnonID() string {
	b.mu.Lock()
	b.anonID++
	id := b.anonID
	b.mu.Unlock()
	return fmt.Sprintf("anon-%d", id)
}

// send to all subscribed clients
func (b *Broker) publishMessage(pkt *packet.PublishPacket) {
	type outbound struct {
		c *Client
		p *packet.PublishPacket
	}
	var targets []outbound

	b.mu.Lock()
	for _, client := range b.clients {
		if client == nil || client.conn == nil || client.stopped() {
			continue
		}
		if client.Contains(pkt.Topic) {
			client.messageId++

			// each subscriber gets its own copy with its own packet id;
			// retain flag must be 0 on live (non-retained) delivery
			cp := *pkt
			cp.Id = client.messageId
			cp.Retain = false

			if cp.QoS > 0 {
				client.ack[fmt.Sprintf("s%d", cp.Id)] = &cp
			}
			targets = append(targets, outbound{client, &cp})
		}
	}
	b.mu.Unlock()

	for _, t := range targets {
		t.c.Send(t.p)
	}
}

// sendWill publishes the client's will message to matching subscribers.
// Called only on abnormal disconnect (a graceful DISCONNECT discards the will).
func (b *Broker) sendWill(client *Client) {
	if client == nil {
		return
	}

	b.mu.RLock()
	will := client.will
	b.mu.RUnlock()
	if will == nil {
		return
	}

	if will.Retain {
		if will.Payload != "" {
			db.SaveRetain(will.Topic, will.Payload, will.QoS.Int())
		} else {
			db.DeleteRetain(will.Topic, will.QoS.Int())
		}
	}

	publish := packet.NewPublish()
	publish.Topic = will.Topic
	publish.Payload = will.Payload
	publish.QoS = will.QoS
	publish.Retain = will.Retain

	b.publishMessage(publish)
}

func (b *Broker) rescan() {
	for {
		time.Sleep(time.Second * 10)
		// TODO try to re-send unacknowledged messages
	}
}

// removeClient drops a client from the registry unless it holds a persistent session.
func (b *Broker) removeClient(id string, client *Client) {
	b.mu.Lock()
	if !client.session {
		delete(b.clients, id)
	}
	b.mu.Unlock()
}

func (b *Broker) broker() {
	for pkt := range b.channel {
		if b.debug {
			log.Printf("broker receive message %s from %s", pkt, pkt.Source())
		}

		b.mu.RLock()
		client := b.clients[pkt.Source()]
		b.mu.RUnlock()

		switch pkt.Type() {
		case packet.PING:
			if client != nil {
				client.Send(packet.NewPong())
			}
		case packet.DISCONNECT:
			// graceful disconnect: discard the will, do not publish it
			if client != nil {
				b.removeClient(pkt.Source(), client)
				client.Stop()
			}
		case packet.SUBSCRIBE:
			if client == nil {
				break
			}
			sp := pkt.(*packet.SubscribePacket)

			res := packet.NewSubAck()
			res.Id = sp.Id

			retains, _ := db.FetchRetain()

			b.mu.Lock()
			for _, payload := range sp.Topics {
				res.ReturnCodes = append(res.ReturnCodes, client.addSubscription(payload))
				if client.session {
					db.SaveSubscription(pkt.Source(), payload.Topic, payload.QoS.Int())
				}
			}

			// collect retained messages matching the new subscriptions
			var retained []*packet.PublishPacket
			for _, m := range retains {
				if client.Contains(m.Topic) {
					client.messageId++
					cp := *m
					cp.Id = client.messageId
					if cp.QoS > 0 {
						client.ack[fmt.Sprintf("s%d", cp.Id)] = &cp
					}
					retained = append(retained, &cp)
				}
			}
			b.mu.Unlock()

			client.Send(res)
			for _, m := range retained {
				client.Send(m)
			}
		case packet.UNSUBSCRIBE:
			if client == nil {
				break
			}
			up := pkt.(*packet.UnSubscribePacket)

			res := packet.NewUnSubAck()
			res.Id = up.Id

			b.mu.Lock()
			for _, payload := range up.Topics {
				if client.removeSubscription(payload) && client.session {
					db.DeleteSubscription(pkt.Source(), payload.Topic)
				}
			}
			b.mu.Unlock()

			client.Send(res)
		case packet.PUBLISH:
			if client == nil {
				break
			}
			pp := pkt.(*packet.PublishPacket)

			if pp.Retain {
				if pp.Payload != "" {
					db.SaveRetain(pp.Topic, pp.Payload, pp.QoS.Int())
				} else {
					db.DeleteRetain(pp.Topic, pp.QoS.Int())
				}
			}

			if pp.DUP {
				// re-delivery of an already in-flight message: nothing to do
				break
			}

			switch pp.QoS {
			case packet.AtMostOnce:
				b.publishMessage(pp)
			case packet.AtLeastOnce:
				puback := packet.NewPubAck()
				puback.Id = pp.Id
				client.Send(puback)
				b.publishMessage(pp)
			case packet.ExactlyOnce:
				pubrec := packet.NewPubRec()
				pubrec.Id = pp.Id

				b.mu.Lock()
				client.ack[fmt.Sprintf("r%d", pp.Id)] = pp
				b.mu.Unlock()

				client.Send(pubrec)
			}
		case packet.PUBACK:
			// answer to our QoS1 publish; ack key prefix is "s"
			if client == nil {
				break
			}
			key := fmt.Sprintf("s%d", pkt.(*packet.PubAckPacket).Id)

			b.mu.Lock()
			p := client.ack[key]
			if p != nil {
				delete(client.ack, key)
			}
			b.mu.Unlock()

			if p != nil {
				log.Printf("%s confirmed", p)
			} else {
				log.Printf("packet to ack %d from %s not found", pkt.(*packet.PubAckPacket).Id, pkt.Source())
			}
		case packet.PUBREC:
			// answer to our QoS2 publish
			if client == nil {
				break
			}
			id := pkt.(*packet.PubRecPacket).Id
			skey := fmt.Sprintf("s%d", id)

			b.mu.Lock()
			p := client.ack[skey]
			if p != nil {
				delete(client.ack, skey)
				client.ack[fmt.Sprintf("l%d", id)] = p
			}
			b.mu.Unlock()

			if p != nil {
				log.Printf("%s confirmed", p)
				pubrel := packet.NewPubRel()
				pubrel.Id = id
				client.Send(pubrel)
			} else {
				log.Printf("packet to rec %d from %s not found", id, pkt.Source())
			}
		case packet.PUBREL:
			// client released a QoS2 publish it sent us: deliver to subscribers
			if client == nil {
				break
			}
			id := pkt.(*packet.PubRelPacket).Id
			rkey := fmt.Sprintf("r%d", id)

			b.mu.Lock()
			p := client.ack[rkey]
			if p != nil {
				delete(client.ack, rkey)
			}
			b.mu.Unlock()

			if p != nil {
				log.Printf("%s confirmed", p)
				b.publishMessage(p.(*packet.PublishPacket))
			} else {
				log.Printf("packet to rel %d from %s not found", id, pkt.Source())
			}

			// always answer with PUBCOMP to complete the handshake
			pubcomp := packet.NewPubComp()
			pubcomp.Id = id
			client.Send(pubcomp)
		case packet.PUBCOMP:
			// answer to our PUBREL: QoS2 delivery to this subscriber is complete
			if client == nil {
				break
			}
			id := pkt.(*packet.PubCompPacket).Id
			lkey := fmt.Sprintf("l%d", id)

			b.mu.Lock()
			p := client.ack[lkey]
			if p != nil {
				delete(client.ack, lkey)
			}
			b.mu.Unlock()

			if p == nil {
				log.Printf("packet to comp %d from %s not found", id, pkt.Source())
			}
		default:
			// sentinel packet: the client disconnected unexpectedly
			log.Println("client unexpectedly disconnected")
			if client != nil {
				b.removeClient(pkt.Source(), client)
				client.Stop()
				// stop first so the will is not delivered back to this client
				b.sendWill(client)
			}
		}
	}
}
