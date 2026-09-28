package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type UnSubscribePacket struct {
	PacketImpl
	Header byte
	Id     uint16
	Topics []SubscribePayload
}

func NewUnSub() *UnSubscribePacket {
	return &UnSubscribePacket{}
}

func CreateUnSubscribe(buf byte) *UnSubscribePacket {
	return &UnSubscribePacket{
		Header: buf,
		Topics: []SubscribePayload{},
	}
}

func (u *UnSubscribePacket) Type() Type {
	return UNSUBSCRIBE
}

func (u *UnSubscribePacket) Length() int {
	var l int
	for _, p := range u.Topics {
		l += p.Length()
	}
	return 2 /*id*/ + l
}

func (u *UnSubscribePacket) Unpack(buf []byte) error {
	id, offset, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}
	u.Id = id

	// UNSUBSCRIBE payload is a list of topic filters only (no QoS byte)
	for offset < len(buf) {
		var topicLen uint16
		var topic string

		topicLen, offset, err = utils.ReadInt16(buf, offset)
		if err != nil {
			return err
		}

		topic, offset, err = utils.ReadString(buf, offset, int(topicLen))
		if err != nil {
			return err
		}

		u.Topics = append(u.Topics, SubscribePayload{Topic: topic})
	}

	return nil
}

func (u *UnSubscribePacket) Pack() []byte {
	lenBuff := WriteLength(u.Length())
	buf := make([]byte, 1+len(lenBuff)+u.Length())

	offset := utils.WriteInt8(buf, 0, byte(UNSUBSCRIBE)<<4)
	offset = utils.WriteBytes(buf, offset, lenBuff)
	offset = utils.WriteInt16(buf, offset, u.Id)

	for _, t := range u.Topics {
		data := t.Pack()
		copy(buf[offset:], data)
		offset += len(data)
	}

	return buf
}

func (u *UnSubscribePacket) String() string {
	var topics string
	for _, t := range u.Topics {
		topics += t.String() + ", "
	}

	return fmt.Sprintf("Unsubscribe: {id: %d, topics: [%s]}", u.Id, topics)
}
