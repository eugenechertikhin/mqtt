package packet

import (
	"fmt"
	"github.com/eugenechertikhin/mqtt/utils"
)

type WillMessage struct {
	Flag      bool
	QoS       QoS
	Retain    bool
	Dublicate bool
	Topic     string
	Payload   string
}

func (m *WillMessage) Type() Type {
	return RESERVED
}

func (m *WillMessage) Length() int {
	return len(m.Topic) + 2 /*topicLen*/ + len(m.Payload) + 2 /*payload len*/
}

func (m *WillMessage) Unpack(buf []byte) error {
	return nil
}

func (m *WillMessage) Pack() []byte {
	lenBuff := WriteLength(m.Length())
	buf := make([]byte, 1+len(lenBuff)+m.Length())

	offset := utils.WriteString(buf, 0, m.Topic)
	offset = utils.WriteString(buf, offset, m.Payload)

	return buf
}

func (m *WillMessage) String() string {
	return fmt.Sprintf(`{"topic":"%s","qos":%d,"retain":"%v","dup":"%v","flag":"%v","payload":"%s"}`,
		m.Topic, m.QoS.Int(), m.Retain, m.Dublicate, m.Flag, m.Payload)
}
