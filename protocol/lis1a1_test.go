package protocol

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/go-bloodlab-net/protocol/utilities"
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
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13, 10}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "tx", bytes: []byte{utilities.ACK}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.STX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte([]byte("2O|1|||||"))})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{utilities.ETX}})
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{57, 68}}) // checksum
	mc.scriptedProtocol = append(mc.scriptedProtocol, scriptedProtocol{receiveOrSend: "rx", bytes: []byte{13, 10}})
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

func TestSendAndReceiveThroughTCPSocket(t *testing.T) {

	impsend := Lis1A1Protocol(DefaultLis1A1ProtocolSettings().EnableFrameNumber())

	testdata := [][]byte{
		[]byte("H|\\^&|||LIS|||||ImmuLINK|||LIS2-A2|20231014162427"),
		[]byte("P|1|"),
		[]byte("O|1|VAL231017_001||^^^Pool_Cell|R||||||N||||Blood^Product"),
		[]byte("L|1|N"),
	}
	driver, instrument := net.Pipe()

	go func() {
		impreceive := Lis1A1Protocol(DefaultLis1A1ProtocolSettings())
		data_receive, err := impreceive.Receive(driver)
		assert.Nil(t, err)

		expected_outpout := []byte{}
		for _, row := range testdata {
			fmt.Printf("Received:%s\n", string(row))
			expected_outpout = append(expected_outpout, row...)
			expected_outpout = append(expected_outpout, 0x0D)
		}

		assert.Equal(t, expected_outpout, data_receive)
	}()

	bytes, err := impsend.Send(instrument, testdata)
	assert.Nil(t, err)
	assert.Equal(t, 135, bytes)

	time.Sleep(10 * time.Second)
}
