package bloodlabnet

import "time"

type ConnectionInstance interface {
	// Send  data directly from instance. This will not work if the instance
	Send(data []byte) (int, error)
	//Receive data directly from instance. This will not work if the instance handles many connections like TCP-Servers
	Receive() ([]byte, error)
	// Run - Main-Loop
	Run(handler Handler)
	// Stop the main-loop of the Run-handler
	Stop()
}

type Session interface {
	IsAlive() bool
	Send(msg []byte) (int, error)
	Receive() ([]byte, error)
	Close() error
	WaitTermination() error
	RemoteAddress() (string, error)
}

type ConnectionAndSessionInstance interface {
	Connect() error
	ConnectionInstance
	Session
}

type Handler interface {
	//DataReceived event is triggered whenever the underlying protocol delivered a complete block(file/transmission) of data
	DataReceived(session Session, fileData []byte, receiveTimestamp time.Time)
	// Connected event is triggered when connection is established. For client as well as for servers. 	For clients in addition every time the connection had
	// to be reestablished
	Connected(session Session)
	// Disconnected event is triggered when connection
	// is terminated.
	//For Servers: when the client ends the session
	// For clients: when the only client connection ends (inc.eof)
	Disconnected(session Session)

	// Error is called from async process (Run) when
	// status messages regarding the connection is available
	Error(session Session, typeOfError ErrorType, err error)
}
