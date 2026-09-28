package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type PubAckPacket struct {
	PacketImpl
	Header byte
	Id     uint16
}

func NewPubAck() *PubAckPacket {
	return &PubAckPacket{}
}

func CreatePubAck(buf byte) *PubAckPacket {
	return &PubAckPacket{
		Header: buf,
	}
}

func (pack *PubAckPacket) Type() Type {
	return PUBACK
}

func (pack *PubAckPacket) Length() int {
	return 2
}

func (pack *PubAckPacket) Unpack(buf []byte) error {
	id, _, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}
	pack.Id = id

	return nil
}

func (pack *PubAckPacket) Pack() []byte {
	offset := 0
	buf := make([]byte, 4)

	offset = utils.WriteInt8(buf, offset, byte(PUBACK)<<4)
	offset = utils.WriteInt8(buf, offset, byte(pack.Length()))
	offset = utils.WriteInt16(buf, offset, pack.Id)

	return buf
}

func (pack *PubAckPacket) String() string {
	return fmt.Sprintf("PubAck: {id: %d}", pack.Id)
}
