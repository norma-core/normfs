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
	"errors"
	"fmt"

	pb "github.com/norma-core/normfs/normfs_go/pb/normfs"
	"github.com/norma-core/normfs/normfs_go/uintn"

	"github.com/google/uuid"
)

var (
	ErrNotConnected     = errors.New("client not connected or setup not complete")
	ErrBufferFull       = errors.New("client request buffer is full")
	ErrRequestTimeout   = errors.New("request timed out waiting for server response")
	ErrConnectionClosed = errors.New("connection closed while waiting for response")
	ErrInvalidResponse  = errors.New("invalid or unexpected response from server")
	ErrServerSide       = errors.New("server returned an error")
	ErrQueueNotFound    = errors.New("queue not found on server")

	ErrReadStreamClosed = errors.New("read stream closed by server or connection error")
	ErrEntryNotFound    = errors.New("entry not found")
)

func (c *client) Enqueue(queueID string, data []byte) (uintn.UintN, error) {
	c.mu.Lock()
	connected := c.connected
	setupDone := c.setupDone
	c.mu.Unlock()

	if !connected || !setupDone {
		return nil, ErrNotConnected
	}

	writeID := c.nextWriteID.Add(1)

	respChan := make(chan *pb.ServerResponseReader, 1)
	c.pendingWritesMu.Lock()
	c.pendingWrites[writeID] = respChan
	c.pendingWritesMu.Unlock()

	defer func() {
		c.pendingWritesMu.Lock()
		delete(c.pendingWrites, writeID)
		c.pendingWritesMu.Unlock()
		close(respChan)
	}()

	request := &pb.ClientRequest{
		Write: &pb.WriteRequest{
			WriteId: writeID,
			QueueId: queueID,
			Packets: [][]byte{data},
		},
	}

	select {
	case c.c2s <- request:
		c.logger.Debug("Enqueue request sent to buffer", "writeID", writeID, "queueID", queueID)
	default:
		c.logger.Warn("Enqueue request failed, client buffer full", "writeID", writeID, "queueID", queueID)
		return nil, ErrBufferFull
	}

	serverRespReader := <-respChan
	if serverRespReader == nil {
		// This indicates failPendingWrites was called due to connection error
		c.logger.Warn("Enqueue failed, connection closed while waiting for response", "writeID", writeID)
		return nil, ErrConnectionClosed
	}

	enqResponse := serverRespReader.GetWrite()

	switch enqResponse.GetResult() {
	case pb.WriteResponse_WR_DONE: // Assuming this is the success enum value from your .proto
		lastIdBytes := enqResponse.GetIds()[0].GetRaw()
		assignedID, err := uintn.FromLEBytes(lastIdBytes)
		if err != nil {
			// panic because this should never happen in normal operation
			c.logger.Error("Failed to convert assigned ID bytes to UintN", "writeID", writeID, "error", err)
			panic(err)
		}
		c.logger.Debug("Enqueue successful", "writeID", writeID, "assignedID", assignedID.String())
		return assignedID, nil
	case pb.WriteResponse_WR_SERVER_ERROR:
		c.logger.Debug("Enqueue failed, server returned an error status", "writeID", writeID)
		return nil, ErrServerSide
	default:
		return nil, errors.New("unexpected response from server: " + enqResponse.GetResult().String())
	}
}

func (c *client) EnqueuePack(queueID string, data [][]byte) ([]uintn.UintN, error) {
	c.mu.Lock()
	connected := c.connected
	setupDone := c.setupDone
	c.mu.Unlock()

	if !connected || !setupDone {
		return nil, ErrNotConnected
	}

	if len(data) == 0 {
		return nil, nil
	}

	writeID := c.nextWriteID.Add(1)

	respChan := make(chan *pb.ServerResponseReader, 1)
	c.pendingWritesMu.Lock()
	c.pendingWrites[writeID] = respChan
	c.pendingWritesMu.Unlock()

	defer func() {
		c.pendingWritesMu.Lock()
		delete(c.pendingWrites, writeID)
		c.pendingWritesMu.Unlock()
		close(respChan)
	}()

	request := &pb.ClientRequest{
		Write: &pb.WriteRequest{
			WriteId: writeID,
			QueueId: queueID,
			Packets: data,
		},
	}

	select {
	case c.c2s <- request:
		c.logger.Debug("EnqueuePack request sent to buffer", "writeID", writeID, "queueID", queueID, "packetCount", len(data))
	default:
		c.logger.Warn("EnqueuePack request failed, client buffer full", "writeID", writeID, "queueID", queueID)
		return nil, ErrBufferFull
	}

	serverRespReader := <-respChan
	if serverRespReader == nil {
		c.logger.Warn("EnqueuePack failed, connection closed while waiting for response", "writeID", writeID)
		return nil, ErrConnectionClosed
	}

	writeResponse := serverRespReader.GetWrite()
	switch writeResponse.GetResult() {
	case pb.WriteResponse_WR_DONE:
		assignedIDs := make([]uintn.UintN, len(data))
		for i, idBytes := range writeResponse.GetIds() {
			id, err := uintn.FromLEBytes(idBytes.GetRaw())
			if err != nil {
				c.logger.Error("Failed to convert assigned ID bytes to UintN", "writeID", writeID, "error", err)
				panic(err)
			}
			assignedIDs[i] = id
		}
		c.logger.Debug("EnqueuePack successful", "writeID", writeID, "assignedCount", len(assignedIDs))
		if len(assignedIDs) != len(data) {
			c.logger.Warn("EnqueuePack returned fewer IDs than packets sent", "writeID", writeID, "expectedCount", len(data), "actualCount", len(assignedIDs))
			return assignedIDs, errors.New("server returned fewer IDs than packets sent")
		}
		return assignedIDs, nil
	case pb.WriteResponse_WR_SERVER_ERROR:
		c.logger.Warn("EnqueuePack failed, server returned an error status", "writeID", writeID)
		return nil, ErrServerSide
	default:
		c.logger.Error("EnqueuePack failed, unhandled server response result", "writeID", writeID, "result", writeResponse.GetResult().String())
		return nil, errors.New("unexpected response from server: " + writeResponse.GetResult().String())
	}
}

func (c *client) ReadFromOffset(queueID string, offset uintn.UintN, limit uint64, step uint64, bufSize uint) *QueueRead {
	if offset == nil {
		qr := &QueueRead{
			Data: make(chan StreamEntry),
			Err:  ErrInvalidResponse,
		}
		close(qr.Data)
		return qr
	}

	return c.read(queueID,
		func(readID uint64) *pb.ClientRequest {
			return &pb.ClientRequest{
				Read: &pb.ReadRequest{
					ReadId:  readID,
					QueueId: queueID,
					Offset:  &pb.Offset{Id: &pb.Id{Raw: offset.ToLEBytes()}, Type: pb.OffsetType_OT_ABSOLUTE},
					Limit:   limit,
					Step:    step,
				},
			}
		},
		bufSize,
	)
}

func (c *client) ReadFromTail(queueID string, offset uintn.UintN, limit uint64, step uint64, bufSize uint) *QueueRead {
	if offset == nil {
		qr := &QueueRead{
			Data: make(chan StreamEntry),
			Err:  ErrInvalidResponse,
		}
		close(qr.Data)
		return qr
	}

	return c.read(queueID,
		func(readID uint64) *pb.ClientRequest {
			return &pb.ClientRequest{
				Read: &pb.ReadRequest{
					ReadId:  readID,
					QueueId: queueID,
					Offset:  &pb.Offset{Id: &pb.Id{Raw: offset.ToLEBytes()}, Type: pb.OffsetType_OT_SHIFT_FROM_TAIL},
					Limit:   limit,
					Step:    step,
				},
			}
		},
		bufSize,
	)
}

func (c *client) read(queueID string, createReq func(readId uint64) *pb.ClientRequest, bufSize uint) *QueueRead {
	streamUUID := uuid.New()
	qr := &QueueRead{
		Id:   streamUUID,
		Data: make(chan StreamEntry), // Unbuffered, will be replaced if bufSize > 0
	}

	c.mu.Lock()
	connected := c.connected
	setupDone := c.setupDone
	c.mu.Unlock()

	if !connected || !setupDone {
		qr.Err = ErrNotConnected
		close(qr.Data)
		return qr
	}

	readID := c.nextReadID.Add(1)
	c.logger.Debug("Initiating Read operation", "readID", readID, "streamUUID", streamUUID.String(), "queueID", queueID, "bufSize", bufSize)

	serverResponseChan := make(chan *pb.ServerResponseReader, messageChanBuffer)

	c.pendingReadsMu.Lock()
	c.pendingReads[readID] = serverResponseChan
	c.pendingReadsMu.Unlock()

	actualBufSize := 1 // Default to 1 if bufSize is 0, to prevent blocking on send if unbuffered
	if bufSize > 0 {
		actualBufSize = int(bufSize)
	}
	// Replace the initial unbuffered channel with one of the correct size
	qr.Data = make(chan StreamEntry, actualBufSize)

	request := createReq(readID)

	select {
	case c.c2s <- request:
		c.logger.Debug("Read request sent to c2s buffer", "readID", readID)
	default:
		c.pendingReadsMu.Lock()
		delete(c.pendingReads, readID)
		c.pendingReadsMu.Unlock()
		close(serverResponseChan) // Clean up the created channel

		qr.Err = ErrBufferFull
		close(qr.Data)
		c.logger.Warn("Read request failed, client c2s buffer full", "readID", readID)
		return qr
	}

	go func() {
		var finalError error // Stores the first error encountered in this stream
		var readedCount int

		defer func() {
			c.pendingReadsMu.Lock()
			delete(c.pendingReads, readID)
			c.pendingReadsMu.Unlock()

			close(qr.Data)            // Always close the data channel
			close(serverResponseChan) // Always close the server response channel

			qr.Err = finalError // Set the final error status

			c.logger.Debug(fmt.Sprintf("Read stream goroutine finished and cleaned up, total reads: %d", readedCount), "readID", readID, "streamUUID", streamUUID.String(), "error", finalError)
		}()

		setFinalError := func(err error) {
			if finalError == nil { // Capture only the first error
				finalError = err
			}
			c.logger.Warn("Read stream error occurred", "readID", readID, "streamUUID", streamUUID.String(), "error", err)
		}

		// Phase 1: Wait for RR_START or an immediate error
		initialResponseReader, ok := <-serverResponseChan
		if !ok || initialResponseReader == nil {
			setFinalError(ErrReadStreamClosed)
			return
		}

		initialReadResp := initialResponseReader.GetRead()
		if initialReadResp == nil {
			setFinalError(ErrInvalidResponse)
			return
		}

		switch initialReadResp.GetResult() {
		case pb.ReadResponse_RR_START:
			c.logger.Debug("Read stream started (RR_START received)", "readID", readID, "streamUUID", streamUUID.String())
		case pb.ReadResponse_RR_QUEUE_NOT_FOUND:
			setFinalError(ErrQueueNotFound)
			return
		case pb.ReadResponse_RR_NOT_FOUND:
			setFinalError(ErrEntryNotFound)
			return
		case pb.ReadResponse_RR_SERVER_ERROR:
			setFinalError(ErrServerSide)
			return
		default:
			err := errors.New("unexpected initial server response: " + initialReadResp.GetResult().String())
			setFinalError(err)
			return
		}

		// Phase 2: Stream entries
		for {
			responseReader, streamOk := <-serverResponseChan
			if !streamOk || responseReader == nil {
				setFinalError(ErrReadStreamClosed)
				return
			}

			readResp := responseReader.GetRead()
			if readResp == nil {
				setFinalError(ErrInvalidResponse)
				return
			}

			var EOL bool // End Of Life for this stream

			switch readResp.GetResult() {
			case pb.ReadResponse_RR_ENTRY:
				readedCount++
				idBytes := readResp.GetId().GetRaw()
				idx, err := uintn.FromLEBytes(idBytes)
				if err != nil {
					// This is a critical internal error, should ideally panic or be logged severely
					c.logger.Error(fmt.Sprintf("Failed to convert ID bytes to UintN: %v", err))
					setFinalError(errors.New("internal error: failed to parse entry ID"))
					EOL = true // Treat as fatal stream error
					break
				}
				entry := StreamEntry{
					ID:         StreamEntryId{ID: idx},
					Data:       readResp.GetData(),
					DataSource: readResp.GetDataSource(),
				}
				qr.Data <- entry

			case pb.ReadResponse_RR_END:
				c.logger.Debug("Read stream finished (RR_END received)", "readID", readID, "streamUUID", streamUUID.String())
				EOL = true
			case pb.ReadResponse_RR_QUEUE_NOT_FOUND:
				setFinalError(ErrQueueNotFound)
				EOL = true
			case pb.ReadResponse_RR_NOT_FOUND:
				setFinalError(ErrEntryNotFound)
				EOL = true
			case pb.ReadResponse_RR_SERVER_ERROR:
				setFinalError(ErrServerSide)
				EOL = true
			default:
				err := errors.New("unexpected response result during stream: " + readResp.GetResult().String())
				setFinalError(err)
				EOL = true
			}

			if EOL {
				return // Exit goroutine, defers will clean up (including setting qr.Err)
			}
		}
	}()

	return qr
}

// Follow reads entries from a queue starting from the tail (offset 0 from tail),
// continuously following new entries. Runs in a goroutine.
// Returns an error channel that receives the final error (or nil).
func (c *client) Follow(queueID string, target chan<- StreamEntry) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		res := c.ReadFromTail(queueID, uintn.FromU8(0), 0, 1, 1)

		for entry := range res.Data {
			target <- entry
		}

		if res.Err != nil {
			errChan <- errors.New("failed to follow queue " + queueID + ": " + res.Err.Error())
			return
		}

		errChan <- nil
	}()

	return errChan
}
