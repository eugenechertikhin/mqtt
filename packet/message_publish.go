package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type PublishPacket struct {
	PacketImpl
	Header  byte
	Id      uint16
	QoS     QoS
	Retain  bool
	DUP     bool
	Topic   string
	Payload string
}

func NewPublish() *PublishPacket {
	return &PublishPacket{}
}

func CreatePublish(buf byte) *PublishPacket {
	return &PublishPacket{
		Header: buf,
	}
}

func (p *PublishPacket) Type() Type {
	return PUBLISH
}

func (p *PublishPacket) Length() int {
	l := 2 /*topic len*/ + len(p.Topic) + len(p.Payload)
	if p.QoS > 0 {
		return l + 2
	}
	return l
}

func (p *PublishPacket) Unpack(buf []byte) error {
	p.DUP = p.Header>>3&0x1 == 1
	p.Retain = p.Header&0x1 == 1
	p.QoS = QoS(p.Header >> 1 & 0x3)

	topicLen, offset, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}

	p.Topic, offset, err = utils.ReadString(buf, offset, int(topicLen))
	if err != nil {
		return err
	}

	if p.QoS > 0 {
		p.Id, offset, err = utils.ReadInt16(buf, offset)
		if err != nil {
			return err
		}
	}

	p.Payload, offset, err = utils.ReadString(buf, offset, len(buf)-offset)
	if err != nil {
		return err
	}

	return nil
}

func (p *PublishPacket) Pack() []byte {
	lenBuff := WriteLength(p.Length())
	buf := make([]byte, 1+len(lenBuff)+p.Length())

	var packetType uint8 = byte(PUBLISH) << 4
	if p.Retain {
		packetType |= 0x01
	}

	if p.DUP {
		packetType |= 0x8
	}

	if p.QoS == QoS(1) {
		packetType |= 0x2
	}

	if p.QoS == QoS(2) {
		packetType |= 0x4
	}
	offset := utils.WriteInt8(buf, 0, packetType)
	offset = utils.WriteBytes(buf, offset, lenBuff)

	offset = utils.WriteString(buf, offset, p.Topic)
	if p.QoS > 0 {
		offset = utils.WriteInt16(buf, offset, p.Id)
	}
	copy(buf[offset:], p.Payload)

	return buf
}

func (p *PublishPacket) String() string {
	return fmt.Sprintf("Publish: {id: %d, topic: %s, payload: %s, qos: %d, retain: %v, dup:%v}",
		p.Id, p.Topic, p.Payload, p.QoS.Int(), p.Retain, p.DUP)
}
