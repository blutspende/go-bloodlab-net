package bloodlabnet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/blutspende/go-bloodlab-net/protocol/utilities"

	"github.com/blutspende/go-bloodlab-net/protocol"
	"github.com/pires/go-proxyproto"
	"github.com/rs/zerolog/log"
)

type tcpServerInstance struct {
	listeningPort      int
	LowLevelProtocol   protocol.Implementation
	connectionType     ConnectionType
	maxConnections     int
	config             TCPServerConfiguration
	isRunning          bool
	sessionCount       int
	listener           net.Listener
	handler            Handler
	mainLoopActive     *sync.WaitGroup
	sessions           []*tcpServerSession
	waitRunningChannel chan bool
}

// --------------------------------------------------------------------------------------------
// BufferedConn wrapping the net.Conn yet compatible for better reading performance and a
// peek preview.
// --------------------------------------------------------------------------------------------
type BufferedConn struct {
	r        *bufio.Reader
	net.Conn // So that most methods are embedded
}

func newBufferedConn(c net.Conn) BufferedConn {
	return BufferedConn{bufio.NewReader(c), c}
}

func newBufferedConnSize(c net.Conn, n int) BufferedConn {
	return BufferedConn{bufio.NewReaderSize(c, n), c}
}

func (b BufferedConn) Peek(n int) ([]byte, error) {
	return b.r.Peek(n)
}

func (b BufferedConn) Read(p []byte) (int, error) {
	return b.r.Read(p)
}

func (b BufferedConn) FirstByteOrError(howLong time.Duration) error {

	if howLong > 0 {
		b.Conn.SetReadDeadline(time.Now().Add(howLong))
		defer b.Conn.SetReadDeadline(time.Time{})
	}
	_, err := b.Peek(1)
	if err != nil {
		if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
			return io.EOF // first byte not received in desired time = disconnect
		} else if opErr, ok := err.(*net.OpError); ok && opErr.Op == "read" {
			return io.EOF
		}
		return err
	}
	return nil
}

func CreateNewTCPServerInstance(listeningPort int, protocolReceiveve protocol.Implementation,
	connectionType ConnectionType, maxConnections int, timingConfigs ...TCPServerConfiguration) ConnectionInstance {

	var timingConfig TCPServerConfiguration
	if len(timingConfigs) > 0 {
		timingConfig = timingConfigs[0]
	} else {
		timingConfig = DefaultTCPServerSettings
	}

	return &tcpServerInstance{
		listeningPort:      listeningPort,
		LowLevelProtocol:   protocolReceiveve,
		maxConnections:     maxConnections,
		connectionType:     connectionType,
		config:             timingConfig,
		sessionCount:       0,
		handler:            nil,
		mainLoopActive:     &sync.WaitGroup{},
		sessions:           make([]*tcpServerSession, 0),
		waitRunningChannel: make(chan bool),
	}

}

func (instance *tcpServerInstance) WaitReady() bool {
	instance.waitRunningChannel <- true
	return true
}

func (instance *tcpServerInstance) Stop() {
	instance.isRunning = false
	instance.listener.Close()
	instance.mainLoopActive.Wait()
}

func (instance *tcpServerInstance) Send(data [][]byte) (int, error) {
	return 0, errors.New("server instance can not send data. What are you looking for, a broadcast to all clients that are connected ? ")
}

func (instance *tcpServerInstance) Receive() ([]byte, error) {
	return nil, errors.New("TCP server can't receive messages. Hint: Use another method")
}

func (instance *tcpServerInstance) Run(handler Handler) {
	var err error
	instance.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", instance.listeningPort))
	if err != nil {
		panic(fmt.Errorf("can not start TCP-Server: %w", err))
	}
	instance.handler = handler

	rand.Seed(time.Now().Unix())

	if instance.connectionType == HAProxySendProxyV2 {
		instance.listener = &proxyproto.Listener{Listener: instance.listener}
	}

	instance.isRunning = true
	instance.mainLoopActive.Add(1)

	for instance.isRunning {

		// if any thread waits for notification that the loop is running, this is it
		select {
		case <-instance.waitRunningChannel:
			// by reading from the channel, this unblocks the writers
		default:
		}

		connection, err := instance.listener.Accept()
		if err != nil {
			if instance.handler != nil && instance.isRunning {
				go instance.handler.Error(nil, ErrorAccept, err)
			}
			continue
		}

		remoteIPAddress, _, _ := net.SplitHostPort(connection.RemoteAddr().String())

		if utilities.Contains(remoteIPAddress, instance.config.BlackListedIPAddresses) {
			connection.Close()
			log.Trace().Str("ip", remoteIPAddress).Msg("incoming connection from blacklisted ip address closed")
			continue
		}
		bufferedConn := newBufferedConn(connection)

		// Portscanners and other players disconnect rather quickly; The first Byte sent breaks this delay
		if instance.config.SessionAfterFirstByte {
			if err := bufferedConn.FirstByteOrError(instance.config.SessionInitiationTimeout); err != nil {
				connection.Close()
				log.Trace().Str("ip", remoteIPAddress).Err(err).Msg("tcp server session initiation timeout reached")
				continue // no error, a connection is regarded as "never established"
			}
		}

		if instance.sessionCount >= instance.maxConnections {
			connection.Close()
			if instance.handler != nil {
				go instance.handler.Error(nil, ErrorMaxConnections, nil)
			}
			log.Warn().Str("remoteIP", remoteIPAddress).Msg("max connection reached, forcing disconnect")
			continue
		}

		session, err := createTcpServerSession(bufferedConn, handler, instance.LowLevelProtocol.NewInstance(), instance.config, remoteIPAddress)
		if err != nil {
			instance.handler.Error(session, ErrorCreateSession, fmt.Errorf("error creating a new TCP session - %w", err))
		} else {
			waitStartup := &sync.Mutex{}
			waitStartup.Lock()
			go func() {
				instance.sessions = append(instance.sessions, session)
				instance.sessionCount++
				waitStartup.Unlock()
				instance.tcpSession(session)
				instance.sessionCount--
				instance.sessions = removeSessionFromList(instance.sessions, session)
			}()
			waitStartup.Lock() // wait for the startup to update the sessioncounter
		}
	}

	for _, x := range instance.sessions {
		x.Close()
	}
	instance.listener.Close()

	instance.handler = nil
	instance.mainLoopActive.Done()
}

func removeSessionFromList(connections []*tcpServerSession, which *tcpServerSession) []*tcpServerSession {

	ret := make([]*tcpServerSession, 0)

	for _, x := range connections {
		if x != which {
			ret = append(ret, x)
		}
	}

	return ret
}

func (instance *tcpServerInstance) FindSessionsByIp(ip string) []Session {
	sessions := make([]Session, 0)

	for _, x := range instance.sessions {
		if x.remoteAddr == ip {
			sessions = append(sessions, x)
		}
	}

	return sessions
}

type tcpServerSession struct {
	conn                BufferedConn
	isRunning           bool
	sessionActive       *sync.WaitGroup
	config              TCPServerConfiguration
	remoteAddr          string
	lowLevelProtocol    protocol.Implementation
	handler             Handler
	blockedForSending   *sync.Mutex
	blockedForReceiving *sync.Mutex
	hasDataToSend       bool
	dataToSend          *[]byte
}

func createTcpServerSession(conn BufferedConn, handler Handler,
	protocolReceive protocol.Implementation,
	timingConfiguration TCPServerConfiguration, remoteAddress string) (*tcpServerSession, error) {

	session := &tcpServerSession{
		conn:                conn,
		isRunning:           true,
		sessionActive:       &sync.WaitGroup{},
		lowLevelProtocol:    protocolReceive,
		config:              timingConfiguration,
		handler:             handler,
		remoteAddr:          remoteAddress,
		blockedForSending:   &sync.Mutex{},
		blockedForReceiving: &sync.Mutex{},
		hasDataToSend:       false,
		dataToSend:          nil,
	}
	return session, nil
}

func (instance *tcpServerInstance) tcpSession(session *tcpServerSession) error {
	log.Debug().Str("ip", session.remoteAddr).Msg("tcp server session started")

	session.sessionActive.Add(1)
	defer session.sessionActive.Done()

	defer session.Close()

	if err := session.handler.Connected(session); err != nil {
		// connection handler declined this session
		return err
	}

	for {

		if !session.isRunning || !instance.isRunning {
			log.Warn().Msg("Exit tcp server in general (bad idea) !!!! ++++ ----")
			break
		}

		data, err := session.lowLevelProtocol.Receive(session.conn)

		if err != nil {
			if err == protocol.Timeout {
				log.Warn().Str("ip", session.remoteAddr).Msg("tcp server session timeout")
				continue // Timeout = keep retrying
			}

			if err == io.EOF {
				// EOF is not an error, its a disconnect in TCP-terms: clean exit
				log.Debug().Str("ip", session.remoteAddr).Msg("tcp server session disconnect")
				session.handler.Disconnected(session)
				session.isRunning = false
				break
			}
			log.Error().Err(err).Str("ip", session.remoteAddr).Msg("tcp server session error")
			session.handler.Error(session, ErrorReceive, err)

		} else {
			// Important detail : the read loop is over when DataReceived event occurs. This means
			// that at this point we can also send data
			log.Debug().Str("ip", session.remoteAddr).Int("length (bytes)", len(data)).Msg("tcp server session data received")
			session.handler.DataReceived(session, data, time.Now())
		}

	}
	log.Debug().Str("ip", session.remoteAddr).Msg("tcp server session ended")
	return nil
}

func (session *tcpServerSession) IsAlive() bool {
	return session.isRunning
}

func (session *tcpServerSession) Send(data [][]byte) (int, error) {
	return session.lowLevelProtocol.Send(session.conn, data)
}

func (session *tcpServerSession) Receive() ([]byte, error) {
	return []byte{}, errors.New("you can not receive messages directly, use the event-handler instead")
}

func (session *tcpServerSession) Close() error {
	if session.IsAlive() {
		if session.handler != nil {
			session.handler.Disconnected(session)
		}
		session.conn.Close()
		session.isRunning = false
		return nil
	}
	return nil
}

func (session *tcpServerSession) WaitTermination() error {
	return errors.New("not implemented yet")
}

func (session *tcpServerSession) RemoteAddress() (string, error) {
	return session.remoteAddr, nil
}
