package protocol

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/blutspende/go-bloodlab-net/protocol/utilities"
	"github.com/rs/zerolog/log"
)

var (
	ReceiverDoesNotRespond   = errors.New("receiver does not react on sent message")
	ReceivedMessageIsInvalid = errors.New("received message is invalid")
)

type Lis1A1ProtocolSettings struct {
	expectFrameNumbers             bool
	strictChecksumValidation       bool
	appendCarriageReturnToFrameEnd bool
	sendTimeoutDuration            time.Duration
	strictFrameOrder               bool
	lineEnding                     []byte
}

func (s Lis1A1ProtocolSettings) EnableStrictChecksum() *Lis1A1ProtocolSettings {
	s.strictChecksumValidation = true
	return &s
}

func (s Lis1A1ProtocolSettings) DisableStrictChecksum() *Lis1A1ProtocolSettings {
	s.strictChecksumValidation = false
	return &s
}

func (s Lis1A1ProtocolSettings) EnableFrameNumber() *Lis1A1ProtocolSettings {
	s.expectFrameNumbers = true
	return &s
}

func (s Lis1A1ProtocolSettings) DisableFrameNumber() *Lis1A1ProtocolSettings {
	s.expectFrameNumbers = false
	return &s
}

func (s Lis1A1ProtocolSettings) EnableStrictFrameOrder() *Lis1A1ProtocolSettings {
	s.strictFrameOrder = true
	return &s
}

func (s Lis1A1ProtocolSettings) DisableStrictFrameOrder() *Lis1A1ProtocolSettings {
	s.strictFrameOrder = false
	return &s
}

func (s Lis1A1ProtocolSettings) EnableAppendCarriageReturnToFrameEnd() *Lis1A1ProtocolSettings {
	s.appendCarriageReturnToFrameEnd = true
	return &s
}
func (s Lis1A1ProtocolSettings) DisableAppendCarriageReturnToFrameEnd() *Lis1A1ProtocolSettings {
	s.appendCarriageReturnToFrameEnd = false
	return &s
}
func (s Lis1A1ProtocolSettings) SetSendTimeOutDuration(timeout time.Duration) *Lis1A1ProtocolSettings {
	s.sendTimeoutDuration = timeout
	return &s
}

func (s Lis1A1ProtocolSettings) SetLineEnding(lineEnding []byte) *Lis1A1ProtocolSettings {
	s.lineEnding = lineEnding
	return &s
}

type ProcessState struct {
	State              int
	LastMessage        string
	LastChecksum       string
	MessageLog         []string
	ProtocolMessage    protocolMessage
	currentFrameNumber string
}

const (
	LineReceived utilities.ActionCode = "LineReceived"
	JustAck      utilities.ActionCode = "JustAck"
	FrameNumber  utilities.ActionCode = "FrameNumber"
)

var Rules []utilities.Rule = []utilities.Rule{
	{FromState: utilities.Init, Symbols: []byte{utilities.EOT}, ToState: utilities.Init, Scan: false},
	// everything except ENQ
	{FromState: utilities.Init, Symbols: []byte{0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, ToState: utilities.Init, Scan: false},
	{FromState: utilities.Init, Symbols: []byte{utilities.ENQ}, ToState: 1, Scan: false, ActionCode: JustAck},

	{FromState: 1, Symbols: []byte{utilities.STX}, ToState: 99, Scan: false},

	{FromState: 99, Symbols: []byte("01234567"), ToState: 100, Scan: true, ActionCode: FrameNumber},
	{FromState: 99, Symbols: utilities.PrintableChars8Bit, ToState: 2, Scan: true, ActionCode: FrameNumber}, //  TODO: exclude 0...7
	{FromState: 100, Symbols: utilities.PrintableChars8Bit, ToState: 2, Scan: true},

	{FromState: 2, Symbols: utilities.PrintableChars8Bit, ToState: 2, Scan: true},
	{FromState: 2, Symbols: []byte{utilities.ETX}, ToState: 10, Scan: false, ActionCode: LineReceived},
	{FromState: 2, Symbols: []byte{utilities.ETB}, ToState: 20, Scan: false, ActionCode: LineReceived},

	{FromState: 20, Symbols: []byte("0123456789ABCDEFabcdef"), ToState: 20, Scan: true},
	{FromState: 20, Symbols: []byte{utilities.CR}, ToState: 21, Scan: false, ActionCode: utilities.CheckSum},
	{FromState: 21, Symbols: []byte{utilities.LF}, ToState: 22, Scan: false, ActionCode: JustAck},
	{FromState: 22, Symbols: []byte{utilities.STX}, ToState: 99, Scan: false},

	{FromState: 10, Symbols: []byte("0123456789ABCDEFabcdef"), ToState: 10, Scan: true},
	{FromState: 10, Symbols: []byte{utilities.CR}, ToState: 11, Scan: false, ActionCode: utilities.CheckSum},
	{FromState: 11, Symbols: []byte{utilities.LF}, ToState: 12, Scan: false, ActionCode: JustAck},
	{FromState: 12, Symbols: []byte{utilities.STX}, ToState: 99, Scan: false},
	{FromState: 12, Symbols: []byte{utilities.EOT}, ToState: utilities.Init, Scan: false, ActionCode: utilities.Finished},
}

type lis1A1 struct {
	settings               *Lis1A1ProtocolSettings
	receiveQ               chan protocolMessage
	receiveThreadIsRunning bool
	state                  ProcessState

	// The waitgroups are used as latches since the read-goroutine
	// is async to sending. When sending, the readroutine must be
	// stopped and sending can not start before that has happened.
	// the read-routine can only continue after sending is completeley
	// finished.
	asyncReadActive sync.WaitGroup
	asyncSendActive sync.WaitGroup
}

func DefaultLis1A1ProtocolSettings() *Lis1A1ProtocolSettings {
	var settings Lis1A1ProtocolSettings
	settings.expectFrameNumbers = true
	settings.strictChecksumValidation = true
	settings.sendTimeoutDuration = 30
	settings.strictFrameOrder = false
	settings.lineEnding = []byte{utilities.CR, utilities.LF}
	return &settings
}

func Lis1A1Protocol(settings ...*Lis1A1ProtocolSettings) Implementation {

	var theSettings *Lis1A1ProtocolSettings
	if len(settings) >= 1 {
		theSettings = settings[0]
	} else {
		theSettings = DefaultLis1A1ProtocolSettings()
	}

	return &lis1A1{
		settings:               theSettings,
		receiveQ:               make(chan protocolMessage),
		receiveThreadIsRunning: false,
		asyncReadActive:        sync.WaitGroup{},
		asyncSendActive:        sync.WaitGroup{},
	}
}

func (proto *lis1A1) NewInstance() Implementation {
	return &lis1A1{
		settings:               proto.settings,
		receiveQ:               make(chan protocolMessage),
		receiveThreadIsRunning: false,
	}
}

var Timeout error = fmt.Errorf("Timeout")

func (proto *lis1A1) Receive(conn net.Conn) ([]byte, error) {

	proto.ensureReceiveThreadRunning(conn)

	select {
	case message := <-proto.receiveQ:
		switch message.Status {
		case DATA:
			return message.Data, nil
		case EOF:
			return []byte{}, io.EOF
		case DISCONNECT:
			return []byte{}, io.EOF
		case ERROR:
			return []byte{}, fmt.Errorf("error while reading - abort receiving data: %s", string(message.Data))
		default:
			return []byte{}, fmt.Errorf("internal error: Invalid status of communication (%d) - abort", message.Status)
		}
	case <-time.After(60 * time.Second):
		// return []byte{}, fmt.Errorf("internal error: Invalid status of communication (%d) - abort", message.Status)
		return []byte{}, Timeout
	}

}

func (proto *lis1A1) transferMessageToHandler(messageLog [][]byte) {
	// looks like message is successfully transferred
	fullMsg := make([]byte, 0)
	for _, messageLine := range messageLog {
		fullMsg = append(fullMsg, []byte(messageLine)...)
		fullMsg = append(fullMsg, utilities.CR)
	}

	proto.receiveQ <- protocolMessage{
		Status: DATA,
		Data:   fullMsg,
	}
}

// asynchronous receive loop
func (proto *lis1A1) ensureReceiveThreadRunning(conn net.Conn) {

	if proto.receiveThreadIsRunning {
		return
	}

	go func() {
		// fmt.Println("Start Receiving Thread")
		proto.receiveThreadIsRunning = true

		proto.state.State = 0 // initial state for FSM
		lastMessage := make([]byte, 0)
		fileBuffer := make([][]byte, 0)

		tcpReceiveBuffer := make([]byte, 4096)
		nextExpectedFrameNumber := 1

		// init state machine
		fsm := utilities.CreateFSM(Rules)
		for {
			conn.SetReadDeadline(time.Time{}) // NO timeout ever

			proto.asyncSendActive.Wait()
			proto.asyncReadActive.Add(1)
			conn.SetDeadline(time.Now().Add(time.Second * 30))
			n, err := conn.Read(tcpReceiveBuffer)
			proto.asyncReadActive.Done()
			if os.Getenv("BNETDEBUG") == "true" {
				fmt.Printf("bnet.lisa1.Receive received %s (%d bytes) (raw: % X)\n", string(tcpReceiveBuffer[:n]), n, tcpReceiveBuffer[:n])
			}
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					fsm.Init()
					continue // on timeout....
				} else if opErr, ok := err.(*net.OpError); ok && opErr.Op == "read" {
					proto.receiveThreadIsRunning = false
					proto.receiveQ <- protocolMessage{
						Status: DISCONNECT,
						Data:   []byte(err.Error()),
					}
					return
				} else if err == io.EOF { // EOF = silent exit
					proto.receiveQ <- protocolMessage{
						Status: DISCONNECT,
						Data:   []byte(err.Error()),
					}
					proto.receiveThreadIsRunning = false
					return
				}

				proto.receiveQ <- protocolMessage{
					Status: DISCONNECT,
					Data:   []byte(err.Error()),
				}
				proto.receiveThreadIsRunning = false
				return
			}

			for _, ascii := range tcpReceiveBuffer[:n] {
				messageBuffer, action, err := fsm.Push(ascii)
				if err != nil {
					proto.receiveQ <- protocolMessage{
						Status: ERROR,
						Data:   []byte(err.Error()),
					}
					proto.receiveThreadIsRunning = false
					return
				}
				switch action {
				case utilities.Ok:
				case utilities.Error:
					// error
					protocolMsg := protocolMessage{
						Status: ERROR,
						Data:   []byte("Internal error"),
					}

					if err != nil {
						err = fmt.Errorf("%w, receiveBuffer : %s", err, string(tcpReceiveBuffer[:n]))
						protocolMsg.Data = []byte(err.Error())
					}

					_, err = conn.Write([]byte{utilities.NAK})
					if err != nil {
						protocolMsg.Data = append(protocolMsg.Data, []byte(err.Error())...)
					}

					proto.receiveQ <- protocolMsg
					proto.receiveThreadIsRunning = false
					return

				case LineReceived:
					// append Data
					lastMessage = messageBuffer
					fileBuffer = append(fileBuffer, lastMessage)
					fsm.ResetBuffer()

				case utilities.CheckSum:

					if proto.settings.strictChecksumValidation {
						currentChecksum := computeChecksum([]byte(proto.state.currentFrameNumber), lastMessage, []byte{utilities.ETX})

						if string(currentChecksum) != string(messageBuffer) {
							protocolMsg := protocolMessage{
								Status: ERROR,
								Data:   []byte(fmt.Sprintf("Invalid Checksum. want: %s given: %s ", string(currentChecksum), string(messageBuffer))),
							}

							_, err = conn.Write([]byte{utilities.NAK})
							if err != nil {
								protocolMsg.Data = append(protocolMsg.Data, []byte(err.Error())...)
							}

							proto.receiveQ <- protocolMsg
							proto.receiveThreadIsRunning = false
							return
						}
					}
					lastMessage = make([]byte, 0)
					fsm.ResetBuffer()

				case utilities.Finished:
					// send fileData
					proto.transferMessageToHandler(fileBuffer)

					// fmt.Println("Exit handler !!!!!!!!!!!!!!!!!")
					// TODO: reinitialize FSM would be sufficient
					//proto.receiveThreadIsRunning = false
					//return
					fileBuffer = make([][]byte, 0)
					fsm.ResetBuffer()
					fsm.Init()
				case JustAck:
					conn.SetDeadline(time.Time{})
					bytes, err := conn.Write([]byte{utilities.ACK})
					if bytes != 1 {
						if os.Getenv("BNETDEBUG") == "true" {
							fmt.Printf("bnet.lisa1.Recieve (error) faled to send 1 byte (ACK'just ack') - ignore\n")
						}
					}
					if err != nil {
						if os.Getenv("BNETDEBUG") == "true" {
							fmt.Printf("bnet.lisa1.Recieve error sending 'just ack' %s\n", err.Error())
						}
						fsm.Init()
					}
				case FrameNumber:
					if proto.settings.strictFrameOrder && string(ascii) != strconv.Itoa(nextExpectedFrameNumber) {
						// Check valid frame number
						protocolMsg := protocolMessage{
							Status: ERROR,
							Data:   []byte(fmt.Sprintf("invalid Frame number. currentFrameNumber: %s expectedFrameNumber: %s", string(ascii), strconv.Itoa(nextExpectedFrameNumber))),
						}
						conn.SetDeadline(time.Time{})
						_, err = conn.Write([]byte{utilities.NAK})
						if err != nil {
							protocolMsg.Data = append(protocolMsg.Data, []byte(err.Error())...)
						}
						proto.receiveQ <- protocolMsg
						proto.receiveThreadIsRunning = false
						return
					}

					matched, err := regexp.MatchString("[0-7]", string(ascii))
					if err != nil {
						println(fmt.Sprintf("Can not use search for number in incoming byte: %s", err.Error()))
					}

					if matched {
						nextExpectedFrameNumber = (nextExpectedFrameNumber + 1) % 8
						proto.state.currentFrameNumber = string(messageBuffer[0])
						fsm.ResetBuffer()
					}
				default:
					protocolMsg := protocolMessage{
						Status: ERROR,
						Data:   []byte("Invalid action code "),
					}

					conn.SetDeadline(time.Time{})
					_, err = conn.Write([]byte{utilities.NAK})
					if err != nil {
						protocolMsg.Data = append(protocolMsg.Data, []byte(err.Error())...)
					}
					proto.receiveQ <- protocolMsg
					proto.receiveThreadIsRunning = false
					fmt.Println("Disconnect due to unexpected, unkown and unlikley error")
					return
				}
			}
		}
	}()
}

func (proto *lis1A1) Interrupt() {
	// not implemented (not required neither)
}

func (proto *lis1A1) sendFrameAndReceiveAnswer(frame []byte, frameNumber int, conn net.Conn) (byte, error) {
	if os.Getenv("BNETDEBUG") == "true" {
		fmt.Printf("bnet.Send Transmit frame '%s' (raw: % X)\n", string(frame), frame)
	}

	// If frame-numbers are used, then here ;)
	frameStr := ""
	if proto.settings.expectFrameNumbers {
		frameStr = fmt.Sprintf("%d", frameNumber)
	}
	frameEnd := []byte{utilities.ETX}
	if proto.settings.appendCarriageReturnToFrameEnd {
		frameEnd = append([]byte{utilities.CR}, frameEnd...)
	}
	checksum := computeChecksum([]byte(frameStr), frame, frameEnd) //  frameNumber is an empty [] of bytes because it's already set to
	_, err := conn.Write([]byte{utilities.STX})
	if err != nil {
		return 0, err
	}

	_, err = conn.Write(append([]byte(frameStr), frame...))
	if err != nil {
		return 0, err
	}

	_, err = conn.Write(frameEnd)
	if err != nil {
		return 0, err
	}
	_, err = conn.Write(checksum)
	if err != nil {
		return 0, err
	}

	_, err = conn.Write(proto.settings.lineEnding)
	if err != nil {
		return 0, err
	}

	receivedMsg, err := proto.receiveSendAnswer(conn)
	if err != nil {
		return 0, err
	}

	return receivedMsg, nil
}

// https://wiki.bloodlab.org/lib/exe/fetch.php?media=listnode:lis1-a.pdf
func (proto *lis1A1) send(conn net.Conn, data [][]byte, recursionDepth int) (int, error) {

	proto.asyncSendActive.Add(1)
	defer proto.asyncSendActive.Done()

	conn.SetReadDeadline(time.Now())
	proto.asyncReadActive.Wait()
	conn.SetReadDeadline(time.Time{}) // Reset timeline

	if recursionDepth > 10 {
		return -1, fmt.Errorf("the receiver does not accept any data")
	}

	_, err := conn.Write([]byte{utilities.ENQ}) // 8.2.4
	if err != nil {
		return -1, err
	}

	for {
		err = conn.SetDeadline(time.Now().Add(time.Second * proto.settings.sendTimeoutDuration))
		if err != nil {
			return -1, ReceiverDoesNotRespond
		}

		recievingMsg := make([]byte, 1)
		n, err := conn.Read(recievingMsg)
		if os.Getenv("BNETDEBUG") == "true" {
			fmt.Printf("bnet.Send Received %s (%d bytes) (raw: % X)\n", string(recievingMsg), n, recievingMsg)
		}

		if err != nil {
			return n, err
		}
		if n == 0 {
			return n, fmt.Errorf("no data from socket, abort")
		}

		if n == 1 {
			switch recievingMsg[0] {
			case utilities.ACK: // 8.2.5
				//  continue operation
			case utilities.NAK: // 8.2.6
				time.Sleep(time.Second * 10)
				return proto.send(conn, data, recursionDepth+1)
			case utilities.ENQ: // 8.2.7.1, 2
				time.Sleep(time.Second)
				return proto.send(conn, data, recursionDepth+1)
			default:
				log.Warn().Msgf("Received unexpected bytes in transmission (ignoring them) : %c ascii: %d\n", recievingMsg[0], recievingMsg[0])
				continue // ignore all characters until ACK / 8.2.5
			}
		}

		if os.Getenv("BNETDEBUG") == "true" {
			fmt.Printf("bnet.Send - Start transferring\n")
		}

		// 8.3 - Transfer Phase
		//maxLengthOfFrame := 63993
		frameNumber := 1
		bytesTransferred := 0
		var checksum []byte
		for _, frame := range data {
			receivedMsg, err := proto.sendFrameAndReceiveAnswer(frame, frameNumber, conn)
			if err != nil {
				return 0, err
			}

			if receivedMsg == utilities.NAK {
				for i := 0; i < 6; i++ {
					time.Sleep(time.Second)
					receivedMsg, err = proto.sendFrameAndReceiveAnswer(frame, frameNumber, conn)
					if err != nil {
						return 0, err
					}
					if receivedMsg != utilities.NAK {
						break
					}
				}
				if receivedMsg == utilities.NAK {
					return 0, fmt.Errorf("frame was not acknowledged by instrument after 6 retries (frameNumber: %d, frame: %s)", frameNumber, frame)
				}
			}
			switch receivedMsg {
			case utilities.ACK:
				bytesTransferred += len(frame)
				bytesTransferred += len(checksum)
				bytesTransferred += 3 // cr, lf stx and endByte
				frameNumber = incrementFrameNumberModulo8(frameNumber)
				continue // was successfully do next
			case utilities.EOT:
				bytesTransferred += len(frame)
				bytesTransferred += len(checksum)
				bytesTransferred += 3        // cr, lf stx and endByte
				return bytesTransferred, nil // Cancel after that one
			default:
				if os.Getenv("BNETDEBUG") == "true" {
					fmt.Printf("bnet.Send Exit due to invalid Message: '%s' (raw: % X)\n", string(receivedMsg), receivedMsg)
				}

				return 0, ReceivedMessageIsInvalid
			}

		}

		_, err = conn.Write([]byte{utilities.EOT})
		if err != nil {
			return -1, err
		}

		if os.Getenv("BNETDEBUG") == "true" {
			fmt.Printf("bnet.Send Transmission sucessfully completed\n")
		}

		return bytesTransferred, nil
	}
}

func incrementFrameNumberModulo8(frameNumber int) int {
	return (frameNumber + 1) % 8
}

func (proto *lis1A1) receiveSendAnswer(conn net.Conn) (byte, error) {
	err := conn.SetDeadline(time.Now().Add(time.Second * proto.settings.sendTimeoutDuration))
	if err != nil {
		return 0, ReceiverDoesNotRespond
	}

	receivingMsg := make([]byte, 1)
	n, err := conn.Read(receivingMsg)
	if os.Getenv("BNETDEBUG") == "true" {
		fmt.Printf("bnet.Send receiveSendAnswer: read %d bytes : %s (raw: % X)\n", n, receivingMsg[:n], receivingMsg[:n])
	}

	if err != nil {
		return 0, err
	}

	switch receivingMsg[0] {
	case utilities.ACK:
		// was successfully do next
		return utilities.ACK, nil
	case utilities.NAK:
		// Last was not successfully do next
		return utilities.NAK, nil
	case utilities.EOT:
		// Cancel after that one
		return utilities.EOT, nil
	default:
		return 0, ReceivedMessageIsInvalid
	}

}

func (proto *lis1A1) Send(conn net.Conn, data [][]byte) (int, error) {
	return proto.send(conn, data, 1)
}

// ComputeChecksum Helper to compute the ASTM-Checksum
func computeChecksum(frameNumber, record, specialChars []byte) []byte {
	sum := int(0)
	for _, b := range frameNumber {
		sum += int(b)
	}
	for _, b := range record {
		sum += int(b)
	}
	for _, b := range specialChars {
		sum += int(b)
	}

	sum = sum % 256
	return []byte(fmt.Sprintf("%02X", sum))
}
