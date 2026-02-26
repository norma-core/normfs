/*
	          .'\   /`.
	         .'.-.`-'.-.`.
	    ..._:   .-. .-.   :_...
	  .'    '-.(o ) (o ).-'    `.
	 :  _    _ _`~(_)~`_ _    _  :
	:  /:   ' .-=_   _=-. `   ;\  :
	:   :|-.._  '     `  _..-|:   :
	 :   `:| |`:-:-.-:-:'| |:'   :
	  `.   `.| | | | | | |.'   .'
	    `.   `-:_| | |_:-'   .'
	      `-._   ````    _.-'
	          ``-------''

Created by ab, 30.05.2025
*/

package normfs_go

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/norma-core/normfs/normfs_go/pb/normfs"
	"github.com/norma-core/normfs/normfs_go/uintn"

	"log/slog"
)

const (
	pingInterval         = 1 * time.Second
	reconnectDelay       = 5 * time.Second
	clientVersion        = 1
	writeTimeout         = 30 * time.Second
	readTimeout          = 30 * time.Second
	maxMessageSize       = 5 * 1024 * 1024 * 1024
	setupResponseTimeout = 10 * time.Second
	messageChanBuffer    = 1024
)

type client struct {
	addr string

	mu                      sync.Mutex
	conn                    net.Conn
	connected               bool
	setupDone               bool
	wg                      sync.WaitGroup
	logger                  *slog.Logger
	lastMsgSentTimeUnixNano atomic.Int64
	pingSequence            uint64

	c2s chan *pb.ClientRequest
	s2c chan *pb.ServerResponseReader

	nextWriteID     atomic.Uint64
	pendingWrites   map[uint64]chan *pb.ServerResponseReader
	pendingWritesMu sync.Mutex

	nextReadID     atomic.Uint64
	nextRawFileId  atomic.Uint64
	pendingReads   map[uint64]chan *pb.ServerResponseReader
	pendingReadsMu sync.Mutex
}

type Client interface {
	Enqueue(queueID string, data []byte) (uintn.UintN, error)
	EnqueuePack(queueID string, data [][]byte) ([]uintn.UintN, error)
	ReadFromOffset(queueID string, offset uintn.UintN, limit uint64, step uint64, bufSize uint) *QueueRead
	ReadFromTail(queueID string, offset uintn.UintN, limit uint64, step uint64, bufSize uint) *QueueRead
	Follow(queueID string, target chan<- StreamEntry) <-chan error
}

func newClient(initialConn net.Conn, addr string, parentLogger *slog.Logger) (Client, error) {
	logger := parentLogger.With(
		"component", "streamsfs-client-internal",
		"server_addr", addr,
	)

	c := &client{
		addr:          addr,
		conn:          initialConn,
		logger:        logger,
		c2s:           make(chan *pb.ClientRequest, messageChanBuffer),
		s2c:           make(chan *pb.ServerResponseReader, messageChanBuffer),
		pendingWrites: make(map[uint64]chan *pb.ServerResponseReader),
		pendingReads:  make(map[uint64]chan *pb.ServerResponseReader),
	}

	c.conn = initialConn
	c.connected = true

	if err := c.performInitialSetup(); err != nil {
		logger.Error("Initial protocol setup failed directly in newClient.", "error", err)
		if c.conn != nil {
			c.conn.Close()
		}
		return nil, fmt.Errorf("initial client setup failed: %w", err)
	}
	logger.Info("Initial client setup successful.")

	c.wg.Add(1)
	go c.manageConnection()

	return c, nil
}

func (c *client) manageConnection() {
	defer c.wg.Done()
	c.logger.Debug("Client connection manager started")

	for {
		c.mu.Lock()
		isConnected := c.connected
		currentConn := c.conn
		c.mu.Unlock()

		if !isConnected || currentConn == nil {
			c.logger.Debug("Attempting to establish connection...")
			newConn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
			if err != nil {
				c.logger.Error("Connection attempt failed", "error", err)
				c.closeConnectionInternal()
				time.Sleep(reconnectDelay)
				continue
			}
			c.logger.Debug("Connection established", "local_addr", newConn.LocalAddr().String(), "remote_addr", newConn.RemoteAddr().String())
			c.mu.Lock()
			c.conn = newConn
			c.connected = true
			c.setupDone = false
			c.mu.Unlock()
		}

		c.mu.Lock()
		needsInitialSetup := c.connected && !c.setupDone
		c.mu.Unlock()

		if needsInitialSetup {
			if err := c.performInitialSetup(); err != nil {
				c.logger.Error("Protocol setup failed after reconnection, will close connection and retry.", "error", err)
				time.Sleep(reconnectDelay)
				continue
			}
			c.logger.Info("Protocol setup successful after reconnection.")
		}

		stopConnectionLoops := make(chan struct{})
		var connActivityWG sync.WaitGroup

		connActivityWG.Add(4)
		go c.readLoop(&connActivityWG, stopConnectionLoops)
		go c.writeLoop(&connActivityWG, stopConnectionLoops)
		go c.keepAliveLoop(&connActivityWG, stopConnectionLoops)
		go c.processResponses(&connActivityWG, stopConnectionLoops) // Will be a stub for now

		connActivityWG.Wait()
		c.logger.Info("Connection activity loops terminated.")
	}
}

func (c *client) performInitialSetup() error {
	c.mu.Lock()
	targetConn := c.conn
	if targetConn == nil || !c.connected {
		c.mu.Unlock()
		err := errors.New("performInitialSetup: no active connection or not connected state")
		c.logger.Error("Pre-condition failed for performInitialSetup", "error", err)
		c.handleReadWriteError(err.Error(), err, nil)
		return err
	}
	c.mu.Unlock()

	setupReq := &pb.ClientRequest{
		Setup: &pb.SetupRequest{Version: clientVersion},
	}
	setupAttemptStopChan := make(chan struct{})
	defer c.tryCloseChannel(setupAttemptStopChan)

	if err := c.sendMessageInternal(setupReq, targetConn, setupAttemptStopChan); err != nil {
		return fmt.Errorf("failed to send setup request: %w", err)
	}

	response, err := c.readMessageWithTimeout(targetConn, setupResponseTimeout, setupAttemptStopChan)
	if err != nil {
		return fmt.Errorf("failed to read setup response: %w", err)
	}

	setupResp := response.GetSetup()
	if setupResp == nil {
		err = errors.New("received non-setup response during setup phase")
		c.logger.Error("Protocol error", "error", err)
		c.handleReadWriteError("performInitialSetup: invalid response", err, setupAttemptStopChan)
		return err
	}

	if setupResp.GetVersion() != clientVersion {
		err = errors.New("mismatched versions in SetupResponse")
		c.logger.Error("Version mismatch", "error", err, "server_version", setupResp.GetVersion(), "client_version", clientVersion)
		c.handleReadWriteError("performInitialSetup: version mismatch", err, setupAttemptStopChan)
		return err
	}

	c.mu.Lock()
	c.setupDone = true
	c.mu.Unlock()
	c.logger.Info("SetupResponse received and validated", "version", setupResp.GetVersion())
	return nil
}

func (c *client) readLoop(wg *sync.WaitGroup, stopConnectionLoops chan struct{}) {
	defer wg.Done()
	c.mu.Lock()
	currentConn := c.conn
	c.mu.Unlock()
	if currentConn == nil {
		c.handleReadWriteError("readLoop: nil connection", errors.New("nil connection at start of readLoop"), stopConnectionLoops)
		return
	}

	for {
		select {
		case <-stopConnectionLoops:
			return
		default:
		}
		msg, err := c.readMessageWithTimeout(currentConn, readTimeout, stopConnectionLoops)
		if err != nil {
			return
		}
		select {
		case c.s2c <- msg:
		case <-stopConnectionLoops:
			return
		}
	}
}

func (c *client) writeLoop(wg *sync.WaitGroup, stopConnectionLoops chan struct{}) {
	defer wg.Done()
	c.mu.Lock()
	currentConn := c.conn
	c.mu.Unlock()
	if currentConn == nil {
		c.handleReadWriteError("writeLoop: nil connection", errors.New("nil connection at start of writeLoop"), stopConnectionLoops)
		return
	}

	for {
		select {
		case <-stopConnectionLoops:
			return
		case msg, ok := <-c.c2s:
			if !ok {
				c.tryCloseChannel(stopConnectionLoops)
				return
			}
			if err := c.sendMessageInternal(msg, currentConn, stopConnectionLoops); err != nil {
				return
			}
		}
	}
}

func (c *client) keepAliveLoop(wg *sync.WaitGroup, stopConnectionLoops chan struct{}) {
	defer wg.Done()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopConnectionLoops:
			return
		case <-ticker.C:
			lastSentNano := c.lastMsgSentTimeUnixNano.Load()
			lastSentTime := time.Unix(0, lastSentNano)
			c.mu.Lock()
			shouldPing := c.connected && c.setupDone && time.Since(lastSentTime) >= pingInterval
			connForPing := c.conn
			c.mu.Unlock()
			if shouldPing {
				if connForPing == nil {
					c.tryCloseChannel(stopConnectionLoops)
					return
				}
				c.sendPing(stopConnectionLoops)
			}
		}
	}
}

func (c *client) sendPing(stopConnectionLoops chan struct{}) {
	select {
	case <-stopConnectionLoops:
		return
	default:
	}
	c.mu.Lock()
	c.pingSequence++
	seq := c.pingSequence
	c.mu.Unlock()
	pingReq := &pb.ClientRequest{Ping: &pb.PingRequest{Sequence: seq}}
	select {
	case c.c2s <- pingReq:
	case <-stopConnectionLoops:
	default:
		c.logger.Warn("sendPing: c2s channel full, ping dropped.")
	}
}

func (c *client) sendMessageInternal(req *pb.ClientRequest, conn net.Conn, stopConnectionLoops chan struct{}) error {
	if conn == nil {
		err := errors.New("sendMessageInternal: connection is nil")
		c.handleReadWriteError(err.Error(), err, stopConnectionLoops)
		return err
	}
	payload := req.Marshal()
	size := uint64(len(payload))
	sizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizeBytes, size)

	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		c.handleReadWriteError("sendMessageInternal: setting write deadline", err, stopConnectionLoops)
		return err
	}
	_, err := conn.Write(sizeBytes)
	if err != nil {
		c.handleReadWriteError("sendMessageInternal: writing size", err, stopConnectionLoops)
		return err
	}
	if size > 0 {
		_, err = conn.Write(payload)
		if err != nil {
			c.handleReadWriteError("sendMessageInternal: writing payload", err, stopConnectionLoops)
			return err
		}
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("sendMessageInternal: failed to reset write deadline", "error", err)
	}
	c.lastMsgSentTimeUnixNano.Store(time.Now().UnixNano())
	return nil
}

func (c *client) readMessageWithTimeout(conn net.Conn, timeout time.Duration, stopConnectionLoops chan struct{}) (*pb.ServerResponseReader, error) {
	if conn == nil {
		err := errors.New("readMessageWithTimeout: connection is nil")
		c.handleReadWriteError(err.Error(), err, stopConnectionLoops)
		return nil, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		c.handleReadWriteError("readMessageWithTimeout: setting read deadline for size", err, stopConnectionLoops)
		return nil, err
	}
	sizeBytes := make([]byte, 8)
	_, err := io.ReadFull(conn, sizeBytes)
	if err != nil {
		c.handleReadWriteError("readMessageWithTimeout: reading size", err, stopConnectionLoops)
		return nil, err
	}
	size := binary.LittleEndian.Uint64(sizeBytes)
	if size > maxMessageSize {
		err = errors.New("message size exceeds maximum")
		c.handleReadWriteError(err.Error(), err, stopConnectionLoops)
		return nil, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		c.handleReadWriteError("readMessageWithTimeout: setting read deadline for payload", err, stopConnectionLoops)
		return nil, err
	}
	var payload []byte
	if size > 0 {
		payload = make([]byte, int(size))
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			c.handleReadWriteError("readMessageWithTimeout: reading payload", err, stopConnectionLoops)
			return nil, err
		}
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		c.logger.Warn("readMessageWithTimeout: failed to reset read deadline", "error", err)
	}
	respReader := pb.NewServerResponseReader()
	if size > 0 {
		if errUnmarshal := respReader.Unmarshal(payload); errUnmarshal != nil {
			c.handleReadWriteError("readMessageWithTimeout: unmarshaling payload", errUnmarshal, stopConnectionLoops)
			return nil, errUnmarshal
		}
	}
	return respReader, nil
}

func (c *client) closeConnectionInternal() {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.logger.Info("Network connection closed internally.")
	}
	c.connected = false
	c.setupDone = false
	c.mu.Unlock()
}

func (c *client) failPendingWrites(reason error) {
	c.pendingWritesMu.Lock()
	defer c.pendingWritesMu.Unlock()

	if len(c.pendingWrites) == 0 {
		return
	}
	c.logger.Info("Failing pending writes.", "error", reason, "count", len(c.pendingWrites))
	for id, ch := range c.pendingWrites {
		select {
		case ch <- nil: // Signal error by sending nil
		default:
			c.logger.Warn("Could not send error to pending write; channel full or receiver gone.", "writeID", id)
		}
		// The Enqueue method's defer is responsible for removing the entry from the map.
	}
	// Clear the map after signaling, as Enqueue's defer might not run if the goroutine is blocked.
	// This ensures the map is clean for a new connection.
	c.pendingWrites = make(map[uint64]chan *pb.ServerResponseReader)
}

func (c *client) handleReadWriteError(contextMsg string, err error, stopConnectionLoops chan struct{}) {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		c.logger.Warn("Network timeout", "error", err, "context", contextMsg)
	} else if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "use of closed network connection") || strings.Contains(err.Error(), "broken pipe") {
		c.logger.Info("Connection closed/EOF/broken pipe", "error", err, "context", contextMsg)
	} else {
		c.logger.Error("Network error", "error", err, "context", contextMsg)
	}
	c.closeConnectionInternal()
	c.tryCloseChannel(stopConnectionLoops)
	connectionError := err
	if errors.Is(err, io.EOF) {
		connectionError = errors.New("connection closed")
	}
	c.failPendingWrites(connectionError)
	c.failPendingReads(connectionError)
}

func (c *client) failPendingReads(reason error) {
	c.pendingReadsMu.Lock()
	defer c.pendingReadsMu.Unlock()

	if len(c.pendingReads) == 0 {
		return
	}
	c.logger.Info("Failing pending reads.", "error", reason, "count", len(c.pendingReads))
	for id, ch := range c.pendingReads {
		select {
		case ch <- nil: // Signal error by sending nil
		default:
			c.logger.Warn("Could not send error to pending read; channel full or receiver gone.", "readID", id)
		}
	}
	c.pendingReads = make(map[uint64]chan *pb.ServerResponseReader)
}

func (c *client) tryCloseChannel(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// processResponses dispatches incoming server responses to pending write and read channels.
func (c *client) processResponses(wg *sync.WaitGroup, stopConnectionLoops chan struct{}) {
	defer wg.Done()
	defer c.logger.Debug("Process responses loop stopped")
	c.logger.Debug("Process responses loop started")

	for {
		select {
		case <-stopConnectionLoops:
			c.logger.Debug("Process responses loop shutting down due to stopConnectionLoops signal.")
			return
		case serverRespReader, ok := <-c.s2c:
			if !ok {
				c.logger.Info("s2c channel closed, processResponses exiting.")
				c.tryCloseChannel(stopConnectionLoops)
				return
			}
			if serverRespReader == nil {
				c.logger.Error("Received nil ServerResponseReader from s2c channel.")
				continue
			}

			// Try to dispatch to pendingWrites
			if writeResp := serverRespReader.GetWrite(); writeResp != nil {
				writeID := writeResp.GetWriteId()
				c.pendingWritesMu.Lock()
				ch, ok := c.pendingWrites[writeID]
				c.pendingWritesMu.Unlock()
				if ok {
					select {
					case ch <- serverRespReader:
						c.logger.Debug("Dispatched write response to pendingWrites.", "writeID", writeID)
					default:
						c.logger.Warn("Failed to dispatch write response, channel full or closed.", "writeID", writeID)
					}
				} else {
					c.logger.Warn("Received write response for unknown writeID.", "writeID", writeID)
				}
			}

			// Try to dispatch to pendingReads
			if readResp := serverRespReader.GetRead(); readResp != nil {
				readID := readResp.GetReadId()
				c.pendingReadsMu.Lock()
				ch, ok := c.pendingReads[readID]
				c.pendingReadsMu.Unlock()

				if ok {
					ch <- serverRespReader
				} else {
					// This could happen if a read stream was cancelled or timed out,
					// and the entry was removed from pendingReads, but messages still arrive from the server.
					c.logger.Warn("Received read response for unknown or closed readID.", "readID", readID, "type", readResp.GetResult().String())
				}
			}

		}
	}
}
