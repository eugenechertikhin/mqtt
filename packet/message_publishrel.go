package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type PubRelPacket struct {
	PacketImpl
	Header byte
	Id     uint16
}

func NewPubRel() *PubRelPacket {
	return &PubRelPacket{}
}

func CreatePubRel(buf byte) *PubRelPacket {
	return &PubRelPacket{
		Header: buf,
	}
}

func (p *PubRelPacket) Type() Type {
	return PUBREL
}

func (p *PubRelPacket) Length() int {
	return 2
}

func (p *PubRelPacket) Unpack(buf []byte) error {
	id, _, err := utils.ReadInt16(buf, 0)
	if err != nil {
		return err
	}
	p.Id = id

	return nil
}

func (p *PubRelPacket) Pack() []byte {
	offset := 0
	buf := make([]byte, 4)

	// PUBREL fixed header flags must be 0b0010 per MQTT 3.1.1
	offset = utils.WriteInt8(buf, offset, byte(PUBREL)<<4|0x02)
	offset = utils.WriteInt8(buf, offset, byte(p.Length()))
	offset = utils.WriteInt16(buf, offset, p.Id)

	return buf
}

func (p *PubRelPacket) String() string {
	return fmt.Sprintf("PubRel: {id: %d}", p.Id)
}
