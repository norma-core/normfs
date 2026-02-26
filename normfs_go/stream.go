package normfs_go

import (
	"github.com/google/uuid"
	pb "github.com/norma-core/normfs/normfs_go/pb/normfs"
	"github.com/norma-core/normfs/normfs_go/uintn"
)

type StreamEntryId struct {
	ID uintn.UintN
}

// DataSource re-exported from protobuf
type DataSource = pb.ReadResponse_DataSource

const (
	DS_NONE       = pb.ReadResponse_DS_NONE
	DS_CLOUD      = pb.ReadResponse_DS_CLOUD
	DS_DISK_STORE = pb.ReadResponse_DS_DISK_STORE
	DS_DISK_WAL   = pb.ReadResponse_DS_DISK_WAL
	DS_MEMORY     = pb.ReadResponse_DS_MEMORY
)

// StreamEntry represents an entry in a stream with its associated metadata.
type StreamEntry struct {
	ID         StreamEntryId
	Data       []byte
	DataSource DataSource
	Err        error
}

// QueueRead represents a read operation from the queue (client-side)
type QueueRead struct {
	Id   uuid.UUID
	Data chan StreamEntry
	Err  error
}
