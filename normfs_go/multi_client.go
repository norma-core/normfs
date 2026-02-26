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

Created by ab, 23.06.2025
*/

package normfs_go

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/norma-core/normfs/normfs_go/uintn"
)

const (
	numReadConnections  = 16
	numWriteConnections = 1
)

// multiClient implements the Client interface using multiple dedicated connections
type multiClient struct {
	addr   string
	logger *slog.Logger

	// Write connection (single connection for all write operations)
	writeConn   *client
	writeConnMu sync.RWMutex

	// Read connections (4 connections for read operations)
	readConns     [numReadConnections]*client
	readConnsMu   sync.RWMutex
	nextReadIndex atomic.Uint32 // Round-robin index for read connections

	// Connection factory
	connFactory func() (*client, error)

	closed bool
	mu     sync.RWMutex
}

// NewClient creates a new client with multiple connections
func NewClient(addr string, logger *slog.Logger) (Client, error) {
	mc := &multiClient{
		addr:   addr,
		logger: logger.With("component", "multi-client"),
	}

	mc.connFactory = func() (*client, error) {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
		}

		c, err := newClient(conn, addr, logger)
		if err != nil {
			conn.Close()
			return nil, err
		}

		internalClient, ok := c.(*client)
		if !ok {
			conn.Close()
			return nil, fmt.Errorf("unexpected client type")
		}

		return internalClient, nil
	}

	if err := mc.initializeConnections(); err != nil {
		mc.Close() // Clean up any partially created connections
		return nil, err
	}

	go mc.monitorConnections()

	return mc, nil
}

// initializeConnections creates all the required connections
func (mc *multiClient) initializeConnections() error {
	writeConn, err := mc.connFactory()
	if err != nil {
		return fmt.Errorf("failed to create write connection: %w", err)
	}
	mc.writeConn = writeConn
	mc.logger.Info("Write connection established")

	for i := 0; i < numReadConnections; i++ {
		readConn, err := mc.connFactory()
		if err != nil {
			return fmt.Errorf("failed to create read connection %d: %w", i, err)
		}
		mc.readConns[i] = readConn
		mc.logger.Info("Read connection established", "index", i)
	}

	return nil
}

// monitorConnections periodically checks and repairs connections
func (mc *multiClient) monitorConnections() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		mc.mu.RLock()
		if mc.closed {
			mc.mu.RUnlock()
			return
		}
		mc.mu.RUnlock()

		mc.writeConnMu.Lock()
		if mc.writeConn == nil || !mc.writeConn.connected || !mc.writeConn.setupDone {
			mc.logger.Warn("Write connection unhealthy, attempting to recreate")
			if newConn, err := mc.connFactory(); err == nil {
				if mc.writeConn != nil {
					mc.writeConn.closeConnectionInternal()
				}
				mc.writeConn = newConn
				mc.logger.Info("Write connection recreated successfully")
			} else {
				mc.logger.Error("Failed to recreate write connection", "error", err)
			}
		}
		mc.writeConnMu.Unlock()

		mc.readConnsMu.Lock()
		for i := 0; i < numReadConnections; i++ {
			if mc.readConns[i] == nil || !mc.readConns[i].connected || !mc.readConns[i].setupDone {
				mc.logger.Warn("Read connection unhealthy, attempting to recreate", "index", i)
				if newConn, err := mc.connFactory(); err == nil {
					if mc.readConns[i] != nil {
						mc.readConns[i].closeConnectionInternal()
					}
					mc.readConns[i] = newConn
					mc.logger.Info("Read connection recreated successfully", "index", i)
				} else {
					mc.logger.Error("Failed to recreate read connection", "error", err, "index", i)
				}
			}
		}
		mc.readConnsMu.Unlock()
	}
}

// getWriteConnection returns the write connection
func (mc *multiClient) getWriteConnection() (*client, error) {
	mc.writeConnMu.RLock()
	defer mc.writeConnMu.RUnlock()

	if mc.writeConn == nil || !mc.writeConn.connected || !mc.writeConn.setupDone {
		return nil, fmt.Errorf("write connection not available")
	}

	return mc.writeConn, nil
}

// getReadConnection returns a read connection using round-robin
func (mc *multiClient) getReadConnection() (*client, error) {
	mc.readConnsMu.RLock()
	defer mc.readConnsMu.RUnlock()

	// Try all connections starting from the next index
	startIndex := mc.nextReadIndex.Add(1) % numReadConnections
	for i := 0; i < numReadConnections; i++ {
		index := (startIndex + uint32(i)) % numReadConnections
		conn := mc.readConns[index]
		if conn != nil && conn.connected && conn.setupDone {
			return conn, nil
		}
	}

	return nil, fmt.Errorf("no healthy read connections available")
}

// Enqueue implements Client.Enqueue using the write connection
func (mc *multiClient) Enqueue(queueID string, data []byte) (uintn.UintN, error) {
	mc.mu.RLock()
	if mc.closed {
		mc.mu.RUnlock()
		return nil, fmt.Errorf("client is closed")
	}
	mc.mu.RUnlock()

	conn, err := mc.getWriteConnection()
	if err != nil {
		return nil, err
	}

	return conn.Enqueue(queueID, data)
}

// EnqueuePack implements Client.EnqueuePack using the write connection
func (mc *multiClient) EnqueuePack(queueID string, data [][]byte) ([]uintn.UintN, error) {
	mc.mu.RLock()
	if mc.closed {
		mc.mu.RUnlock()
		return nil, fmt.Errorf("client is closed")
	}
	mc.mu.RUnlock()

	conn, err := mc.getWriteConnection()
	if err != nil {
		return nil, err
	}

	return conn.EnqueuePack(queueID, data)
}

// ReadFromOffset implements Client.ReadFromOffset using a read connection
func (mc *multiClient) ReadFromOffset(queueID string, offset uintn.UintN, limit uint64, step uint64, bufSize uint) *QueueRead {
	mc.mu.RLock()
	if mc.closed {
		mc.mu.RUnlock()
		qr := &QueueRead{
			Data: make(chan StreamEntry),
			Err:  fmt.Errorf("client is closed"),
		}
		close(qr.Data)
		return qr
	}
	mc.mu.RUnlock()

	conn, err := mc.getReadConnection()
	if err != nil {
		qr := &QueueRead{
			Data: make(chan StreamEntry),
			Err:  err,
		}
		close(qr.Data)
		return qr
	}

	return conn.ReadFromOffset(queueID, offset, limit, step, bufSize)
}

// ReadFromTail implements Client.ReadFromTail using a read connection
func (mc *multiClient) ReadFromTail(queueID string, offset uintn.UintN, limit uint64, step uint64, bufSize uint) *QueueRead {
	mc.mu.RLock()
	if mc.closed {
		mc.mu.RUnlock()
		qr := &QueueRead{
			Data: make(chan StreamEntry),
			Err:  fmt.Errorf("client is closed"),
		}
		close(qr.Data)
		return qr
	}
	mc.mu.RUnlock()

	conn, err := mc.getReadConnection()
	if err != nil {
		qr := &QueueRead{
			Data: make(chan StreamEntry),
			Err:  err,
		}
		close(qr.Data)
		return qr
	}

	return conn.ReadFromTail(queueID, offset, limit, step, bufSize)
}

// Follow implements Client.Follow using a read connection
func (mc *multiClient) Follow(queueID string, target chan<- StreamEntry) <-chan error {
	mc.mu.RLock()
	if mc.closed {
		mc.mu.RUnlock()
		errChan := make(chan error, 1)
		errChan <- fmt.Errorf("client is closed")
		close(errChan)
		return errChan
	}
	mc.mu.RUnlock()

	conn, err := mc.getReadConnection()
	if err != nil {
		errChan := make(chan error, 1)
		errChan <- err
		close(errChan)
		return errChan
	}

	return conn.Follow(queueID, target)
}

// Close closes all connections
func (mc *multiClient) Close() error {
	mc.mu.Lock()
	if mc.closed {
		mc.mu.Unlock()
		return nil
	}
	mc.closed = true
	mc.mu.Unlock()

	mc.writeConnMu.Lock()
	if mc.writeConn != nil {
		mc.writeConn.closeConnectionInternal()
		mc.writeConn = nil
	}
	mc.writeConnMu.Unlock()

	mc.readConnsMu.Lock()
	for i := 0; i < numReadConnections; i++ {
		if mc.readConns[i] != nil {
			mc.readConns[i].closeConnectionInternal()
			mc.readConns[i] = nil
		}
	}
	mc.readConnsMu.Unlock()

	mc.logger.Info("Multi-client closed")
	return nil
}

// GetConnectionStats returns statistics about the connections
func (mc *multiClient) GetConnectionStats() ConnectionStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stats := ConnectionStats{
		TotalConnections: numReadConnections + numWriteConnections,
	}

	mc.writeConnMu.RLock()
	if mc.writeConn != nil && mc.writeConn.connected && mc.writeConn.setupDone {
		stats.HealthyWriteConnections = 1
	}
	mc.writeConnMu.RUnlock()

	mc.readConnsMu.RLock()
	for i := 0; i < numReadConnections; i++ {
		if mc.readConns[i] != nil && mc.readConns[i].connected && mc.readConns[i].setupDone {
			stats.HealthyReadConnections++
		}
	}
	mc.readConnsMu.RUnlock()

	return stats
}

// ConnectionStats holds statistics about the multi-client connections
type ConnectionStats struct {
	TotalConnections        int
	HealthyReadConnections  int
	HealthyWriteConnections int
}
