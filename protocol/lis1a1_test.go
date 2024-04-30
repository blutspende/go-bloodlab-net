package protocol

import (
	"fmt"
	"os"
	"testing"

	"github.com/blutspende/go-bloodlab-net/protocol/utilities"
	"github.com/stretchr/testify/assert"
)

func TestComputeChecksum(t *testing.T) {
	message := "This is transmission text for which we need a checksum"
	frameNumber := "1"
	specialChars := []byte{utilities.ETX}

	expectedChecksum := []byte(fmt.Sprintf("%02X", 61))
	checksum := computeChecksum([]byte(frameNumber), []byte(message), specialChars)

	assert.Equal(t, expectedChecksum, checksum)

	frameNumber = "2"
	checksum = computeChecksum([]byte(frameNumber), []byte(message), specialChars)
	assert.NotEqual(t, expectedChecksum, checksum)
}

// In this test an additional CR is added before ETX (this is something the Becom(Biorad) would need)
func TestComputeChecksumWithCRenclosedInMessage(t *testing.T) {

	specialChars := []byte{utilities.ETX}
	message := "R|1|^^^XM^PR80^KP: IAT(5053)^|0^^|C||||R||Kramer^|20230923114324|20230923123752|3|Manual workplace|1|Kramer\r"
	checksum := computeChecksum([]byte("6"), []byte(message), specialChars)

	assert.Equal(t, []byte(fmt.Sprintf("%02X", 0x37)), checksum)
}

func TestSendData(t *testing.T) {

	// this is like the instrument would behave
	var mc mockConnection
	mc.scriptedProtocol = make([]scriptedProtocol, 0)
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ENQ}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte([]byte("1H||||"))})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{54, 67}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte([]byte("2O|1|||||"))})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{57, 68}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.EOT}})

	mc.currentRecord = 0

	// this is "our" sid of the protocol
	message := [][]byte{}
	message = append(message, []byte("H||||"))
	message = append(message, []byte("O|1|||||"))

	os.Setenv("PROTOLOG_ENABLE", "true") // enable logging
	instance := Logger(Lis1A1Protocol(DefaultLis1A1ProtocolSettings()))

	_, err := instance.Send(&mc, message)

	assert.Nil(t, err)
}

func TestSendDataWithCarriageReturnAppendEnabled(t *testing.T) {
	message := [][]byte{
		[]byte("P|1|12202437932|||^||||||||"),
		[]byte("O|1|12202437932||^^^HSV-M||20240312133412|||||||||||||||||||Q"),
		[]byte("O|2|12202437932||^^^HSV-G||20240312133412|||||||||||||||||||Q"),
		[]byte("L|1|N"),
	}
	var mc mockConnection
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ENQ}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: append([]byte("1"), message[0]...)})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.CR, utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{57, 70}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: append([]byte("2"), message[1]...)})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.CR, utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{65, 51}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: append([]byte("3"), message[2]...)})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.CR, utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{57, 70}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: append([]byte("4"), message[3]...)})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.CR, utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{48, 55}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.EOT}})

	mc.currentRecord = 0

	instance := Logger(Lis1A1Protocol(DefaultLis1A1ProtocolSettings().EnableAppendCarriageReturnToFrameEnd()))
	_, err := instance.Send(&mc, message)

	assert.Nil(t, err)
}

func TestFrameNumber(t *testing.T) {
	assert.Equal(t, 1, incrementFrameNumberModulo8(0))
	assert.Equal(t, 2, incrementFrameNumberModulo8(1))
	assert.Equal(t, 3, incrementFrameNumberModulo8(2))
	assert.Equal(t, 4, incrementFrameNumberModulo8(3))
	assert.Equal(t, 5, incrementFrameNumberModulo8(4))
	assert.Equal(t, 6, incrementFrameNumberModulo8(5))
	assert.Equal(t, 7, incrementFrameNumberModulo8(6))
	assert.Equal(t, 0, incrementFrameNumberModulo8(7))
	assert.Equal(t, 1, incrementFrameNumberModulo8(8))
	assert.Equal(t, 2, incrementFrameNumberModulo8(9))
	assert.Equal(t, 3, incrementFrameNumberModulo8(10))
	assert.Equal(t, 4, incrementFrameNumberModulo8(11))
	assert.Equal(t, 5, incrementFrameNumberModulo8(12))
	assert.Equal(t, 6, incrementFrameNumberModulo8(13))
	assert.Equal(t, 7, incrementFrameNumberModulo8(14))
	assert.Equal(t, 0, incrementFrameNumberModulo8(15))
}

func TestCheckSum(t *testing.T) {
	//1H|\^&|||LIAISONXL|||||LABEDV||P|1|<CR><ETX>97<CR><LF>
	result := computeChecksum([]byte("1"), []byte("H|\\^&|||LIAISONXL|||||LABEDV||P|1|"), []byte{utilities.CR, utilities.ETX})
	assert.Equal(t, "97", string(result))
}

func TestSendDataCustomLineEnding(t *testing.T) {
	var mc mockConnection
	mc.scriptedProtocol = make([]scriptedProtocol, 0)
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ENQ}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte(("1H||||"))})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{54, 67}}) // checksum
	// LF used as line ending
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.LF}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte(("2O|1|||||"))})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{57, 68}}) // checksum
	// line ending
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.LF}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.EOT}})

	mc.currentRecord = 0

	// this is "our" sid of the protocol
	message := [][]byte{}
	message = append(message, []byte("H||||"))
	message = append(message, []byte("O|1|||||"))

	os.Setenv("PROTOLOG_ENABLE", "true") // enable logging
	instance := Logger(Lis1A1Protocol(DefaultLis1A1ProtocolSettings().SetLineEnding([]byte{utilities.LF})))

	_, err := instance.Send(&mc, message)

	assert.Nil(t, err)
}
