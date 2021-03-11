package libp2p

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/suborbital/grav/grav"
	"github.com/suborbital/vektor/vlog"

	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	quic "github.com/libp2p/go-libp2p-quic-transport"
	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-tcp-transport"
)

// MessageSizeMax is a soft (recommended) maximum for network messages.
// One can write more, as the interface is a stream. But it is useful
// to bunch it up into multiple read/writes when the whole message is
// a single, large serialized object.
const MessageSizeMax = 1 << 22 // 4 MB

// List of protocols supported by the transport
var (
	gravProtocol          protocol.ID = "/grav/0.0.1"
	gravHandshakeProtocol protocol.ID = "/grav/handshake/0.0.1"
)

// Transport is a transport that connects Grav nodes using libp2p
type Transport struct {
	opts               *grav.TransportOpts
	log                *vlog.Logger
	h                  host.Host
	ctx                context.Context
	cancel             context.CancelFunc
	supportedProtocols []protocol.ID

	connectionFunc func(grav.Connection)
}

// Conn implements transport.Connection and represents a libp2p connection
type Conn struct {
	nodeUUID string
	log      *vlog.Logger
	// libp2p stream
	s network.Stream

	recvFunc grav.ReceiveFunc
}

// New creates a new libp2p transport
func New() *Transport {
	t := &Transport{}
	t.supportedProtocols = []protocol.ID{
		gravProtocol,
	}

	return t
}

// HTTPHandlerFunc returns an http.HandlerFunc for incoming connections
func (t *Transport) HTTPHandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	}

}

// Setup sets up the transport
func (t *Transport) Setup(opts *grav.TransportOpts, connFunc grav.ConnectFunc, findFunc grav.FindFunc) error {
	t.ctx, t.cancel = context.WithCancel(context.Background())

	transports := libp2p.ChainOptions(
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
	)

	// Randomly choose port if it hasn't been set
	var (
		ip   = "0.0.0.0"
		port = "0"
	)

	if opts.URI != "" {
		port = opts.URI
	}

	if opts.Port != "" {
		port = opts.Port
	}

	listenAddrs := libp2p.ListenAddrStrings(
		fmt.Sprintf("/ip4/%s/tcp/%s", ip, port),
		// Using the next port for the websocket transport
		fmt.Sprintf("/ip4/%s/udp/%s/quic", ip, port),
	)

	// Starting the DHT for discovery even if it's not implemented yet.
	var dht *kaddht.IpfsDHT
	newDHT := func(h host.Host) (routing.PeerRouting, error) {
		var err error
		dht, err = kaddht.New(t.ctx, h)
		return dht, err
	}
	routing := libp2p.Routing(newDHT)

	var err error
	t.h, err = libp2p.New(
		t.ctx,
		transports,
		listenAddrs,
		routing,
	)
	if err != nil {
		return err
	}

	t.opts = opts
	t.log = opts.Logger
	t.connectionFunc = connFunc

	return nil
}

// CreateConnection gets libp2p connection with other peer
func (t *Transport) CreateConnection(endpoint string) (grav.Connection, error) {

	// Get multiaddress
	targetAddr, err := multiaddr.NewMultiaddr(endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "[transport-libp2p] failed to to parse multiaddr from multiaddr")
	}

	targetInfo, err := peer.AddrInfoFromP2pAddr(targetAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "[transport-libp2p] failed to to generate addrinfo")
	}

	// Check if we are already connected to the peer, if not connect to it.
	if t.h.Network().Connectedness(targetInfo.ID) != network.Connected {
		err = t.h.Connect(t.ctx, *targetInfo)
		if err != nil {
			return nil, errors.Wrapf(err, "[transport-libp2p] failed to connect to peer")
		}
	}

	// Start Grav streams
	s, err := t.h.NewStream(t.ctx, targetInfo.ID, t.supportedProtocols...)
	if err != nil {
		return nil, errors.Wrapf(err, "[transport-libp2p] couldn't create stream with peer")
	}

	// Return the connection with the stream
	conn := &Conn{
		log: t.log,
		s:   s,
	}

	// Register streamHandler in host
	t.h.SetStreamHandler(gravProtocol, conn.handleGravStream)
	// TODO: Implement handshake
	// t.h.SetStreamHandler(gravHandshakeProtocol, conn.handleGravStream)

	return conn, nil
}

// Stream handler for Grav Protocol
func (c *Conn) handleGravStream(s network.Stream) {
	c.log.Debug("[transport-libp2p] got a new stream", c.nodeUUID)

	// Defer closing the stream and create reader
	defer c.s.Close()
	r := msgio.NewVarintReaderSize(s, MessageSizeMax)

	// Reading loop
	for {
		// Only accept messages after c.Start() has been called and recvFun has been set
		if c.recvFunc != nil {
			c.log.Debug("[transport-libp2p] recieved message via", c.nodeUUID)
			message, err := r.ReadMsg()
			if err != nil {
				if err != io.EOF {
					// Reset stream if there was error
					_ = s.Reset()
					// TODO: We should maybe notify of this error.
					c.log.Warn("[transport-libp2p] error receiving message from network")
					return
				}
			}

			msg, err := grav.MsgFromBytes(message)
			if err != nil {
				c.log.Error(errors.Wrap(err, "[transport-libp2p] failed to MsgFromBytes"))
				continue
			}

			// send to the Grav instance
			c.recvFunc(msg)
			// Signal that buffer can be reused
			r.ReleaseMsg(message)
		}
	}
}

// Start begins the receiving of messages
func (c *Conn) Start(recvFunc grav.ReceiveFunc) {
	// Register recvFunc in stream. The streamHandler takes it from here!
	c.recvFunc = recvFunc
}

// WriteMessage to wire
func (c *Conn) WriteMessage(msgBytes []byte) error {

	size := len(msgBytes)

	// Get buffer from pool
	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)
	// Size prefix for the stream
	n := binary.PutUvarint(buf, uint64(size))
	// Write message in buffer
	buf = append(buf, msgBytes...)
	// Send message
	_, err := c.s.Write(buf[:n])

	return err
}

// Send sends a message to the connection
func (c *Conn) Send(msg grav.Message) error {
	msgBytes, err := msg.Marshal()
	if err != nil {
		// not exactly sure what to do here (we don't want this going into the dead letter queue)
		// what about trying a re-transmission?
		c.log.Error(errors.Wrap(err, "[transport-libp2p] failed to Marshal message"))
		return nil
	}

	c.log.Debug("[transport-libp2p] sending message to connection", c.nodeUUID)

	if err := c.WriteMessage(msgBytes); err != nil {
		// TODO: Check if stream is closed to return the right error
		// return grav.ErrConnectionClosed
		return errors.Wrap(err, "[transport-libp2p] failed to WriteMessage")
	}

	c.log.Debug("[transport-libp2p] sent message to connection", c.nodeUUID)

	return nil
}

// CanReplace returns true if the connection can be replaced
func (c *Conn) CanReplace() bool {
	return false
}

// TODO: Handshake not implemented yet...
// DoOutgoingHandshake performs a connection handshake and returns the UUID of the node that we're connected to
// so that it can be validated against the UUID that was provided in discovery (or if none was provided)
func (c *Conn) DoOutgoingHandshake(handshake *grav.TransportHandshake) (*grav.TransportHandshakeAck, error) {
	/*
		handshakeJSON, err := json.Marshal(handshake)
		if err != nil {
			return nil, errors.Wrap(err, "failed to Marshal handshake JSON")
		}

		c.log.Debug("[transport-websocket] sending handshake")

		if err := c.WriteMessage(grav.TransportMsgTypeHandshake, handshakeJSON); err != nil {
			return nil, errors.Wrap(err, "failed to WriteMessage handshake")
		}

		mt, message, err := c.conn.ReadMessage()
		if err != nil {
			return nil, errors.Wrap(err, "failed to ReadMessage for handshake ack, terminating connection")
		}

		if mt != grav.TransportMsgTypeHandshake {
			return nil, errors.New("first message recieved was not handshake ack")
		}

		c.log.Debug("[transport-websocket] recieved handshake ack")

		ack := grav.TransportHandshakeAck{}
		if err := json.Unmarshal(message, &ack); err != nil {
			return nil, errors.Wrap(err, "failed to Unmarshal handshake ack")
		}

		c.nodeUUID = ack.UUID
	*/

	return &grav.TransportHandshakeAck{}, nil
}

// DoIncomingHandshake performs a connection handshake and returns the UUID of the node that we're connected to
// so that it can be validated against the UUID that was provided in discovery (or if none was provided)
func (c *Conn) DoIncomingHandshake(handshakeAck *grav.TransportHandshakeAck) (*grav.TransportHandshake, error) {
	/*
		mt, message, err := c.conn.ReadMessage()
		if err != nil {
			return nil, errors.Wrap(err, "failed to ReadMessage for handshake, terminating connection")
		}

		if mt != grav.TransportMsgTypeHandshake {
			return nil, errors.New("first message recieved was not handshake")
		}

		c.log.Debug("[transport-websocket] recieved handshake")

		handshake := grav.TransportHandshake{}
		if err := json.Unmarshal(message, &handshake); err != nil {
			return nil, errors.Wrap(err, "failed to Unmarshal handshake")
		}

		ackJSON, err := json.Marshal(handshakeAck)
		if err != nil {
			return nil, errors.Wrap(err, "failed to Marshal handshake JSON")
		}

		c.log.Debug("[transport-websocket] sending handshake ack")

		if err := c.WriteMessage(grav.TransportMsgTypeHandshake, ackJSON); err != nil {
			return nil, errors.Wrap(err, "failed to WriteMessage handshake ack")
		}

		c.log.Debug("[transport-websocket] sent handshake ack")

		c.nodeUUID = handshake.UUID
	*/
	return &grav.TransportHandshake{}, nil
}

// Close closes the underlying connection
func (c *Conn) Close() {
	c.log.Debug("[transport-libp2p] connection for", c.nodeUUID, "is closing")
	c.s.Close()
}
