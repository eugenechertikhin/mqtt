package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type SubscribePayload struct {
	QoS   QoS
	Topic string
}

func (p *SubscribePayload) Length() int {
	return 2 /*topic len*/ +
		len(p.Topic) +
		1 /*qos*/
}

func (p *SubscribePayload) Pack() []byte {
	buf := make([]byte, p.Length())
	offset := utils.WriteString(buf, 0, p.Topic)
	utils.WriteInt8(buf, offset, uint8(p.QoS))
	return buf
}

func (p *SubscribePayload) String() string {
	return fmt.Sprintf("{topic: %s, qos: %d}", p.Topic, p.QoS.Int())
}

type SubscribePacket struct {
	PacketImpl
	Header byte
	Id     uint16
	Topics []SubscribePayload
}

func NewSubscribe() *SubscribePacket {
	return &SubscribePacket{}
}

func CreateSubscribe(buf byte) *SubscribePacket {
	return &SubscribePacket{
		Header: buf,
		Topics: []SubscribePayload{},
	}
}

func (s *SubscribePacket) Type() Type {
	return SUBSCRIBE
}

func (s *SubscribePacket) Length() int {
	var l int
	for _, p := range s.Topics {
		l += p.Length()
	}
	return 2 /*id*/ + l
}

func (s *SubscribePacket) Unpack(buf []byte) error {
	id, offset, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}
	s.Id = id

	for offset < len(buf) {
		var topicLen uint16
		topicLen, offset, err = utils.ReadInt16(buf, offset)
		if err != nil {
			return err
		}

		var topic string
		topic, offset, err = utils.ReadString(buf, offset, int(topicLen))
		if err != nil {
			return err
		}

		var qos uint8
		qos, offset, err = utils.ReadInt8(buf, offset)
		if err != nil {
			return err
		}

		s.Topics = append(s.Topics, SubscribePayload{Topic: topic, QoS: QoS(qos)})
	}

	return nil
}

func (s *SubscribePacket) Pack() []byte {
	lenBuff := WriteLength(s.Length())
	buf := make([]byte, 1+len(lenBuff)+s.Length())

	offset := utils.WriteInt8(buf, 0, byte(SUBSCRIBE)<<4)
	offset = utils.WriteBytes(buf, offset, lenBuff)
	offset = utils.WriteInt16(buf, offset, s.Id)

	for _, t := range s.Topics {
		data := t.Pack()
		copy(buf[offset:], data)
		offset += len(data)
	}

	return buf
}

func (s *SubscribePacket) String() string {
	var topics string
	for _, t := range s.Topics {
		topics += t.String() + ", "
	}
	return fmt.Sprintf("Subscribe: {id: %d, topics: [%s]}", s.Id, topics)
}
