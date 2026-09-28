package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type PubRecPacket struct {
	PacketImpl
	Header byte
	Id     uint16
}

func NewPubRec() *PubRecPacket {
	return &PubRecPacket{}
}

func CreatePubRec(buf byte) *PubRecPacket {
	return &PubRecPacket{
		Header: buf,
	}
}

func (p *PubRecPacket) Type() Type {
	return PUBREC
}

func (p *PubRecPacket) Length() int {
	return 2
}

func (p *PubRecPacket) Unpack(buf []byte) error {
	id, _, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}
	p.Id = id

	return nil
}

func (p *PubRecPacket) Pack() []byte {
	offset := 0
	buf := make([]byte, 4)

	offset = utils.WriteInt8(buf, offset, byte(PUBREC)<<4)
	offset = utils.WriteInt8(buf, offset, byte(p.Length()))
	offset = utils.WriteInt16(buf, offset, p.Id)

	return buf
}

func (p *PubRecPacket) String() string {
	return fmt.Sprintf("PubRec: {id: %d}", p.Id)
}
