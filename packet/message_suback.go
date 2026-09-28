package packet

import (
	"encoding/binary"
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type SubAckPacket struct {
	PacketImpl
	Header      byte
	Id          uint16
	ReturnCodes []QoS
}

func NewSubAck() *SubAckPacket {
	return &SubAckPacket{}
}

func CreateSubAck(buf byte) *SubAckPacket {
	return &SubAckPacket{
		Header:      buf,
		ReturnCodes: []QoS{},
	}
}

func (sack *SubAckPacket) Type() Type {
	return SUBACK
}

func (sack *SubAckPacket) Length() int {
	return 2 + len(sack.ReturnCodes)
}

func (sack *SubAckPacket) Unpack(buf []byte) error {
	id, offset, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}
	sack.Id = id

	for left := len(buf) - 2; left > 0; left-- {
		var qos byte
		qos, offset, err = utils.ReadInt8(buf, offset)
		sack.ReturnCodes = append(sack.ReturnCodes, QoS(qos))
	}

	return nil
}

func (sack *SubAckPacket) Pack() []byte {
	buf := make([]byte, 4)

	buf[0] = byte(SUBACK) << 4
	buf[1] = byte(sack.Length())
	binary.BigEndian.PutUint16(buf[2:], sack.Id)

	for _, rc := range sack.ReturnCodes {
		buf = append(buf, byte(rc))
	}

	return buf
}

func (sack *SubAckPacket) String() string {
	var codes string
	for _, rc := range sack.ReturnCodes {
		codes += rc.String() + ", "
	}

	return fmt.Sprintf("SubAck: {id: %d, codes: [%s]}", sack.Id, codes)
}
