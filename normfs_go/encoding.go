package normfs_go

import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
	pb "github.com/norma-core/normfs/normfs_go/pb/normfs"
)

// Sent with every read: the server compresses only queues with a rule for it.
var acceptEncodings = []pb.Encoding{pb.Encoding_ENC_ZSTD}

// Bounds what a single entry may claim to decode to.
const maxDecodedEntry = 256 << 20

var zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxDecodedEntry))

func decodeEntry(resp *pb.ReadResponseReader) ([]byte, error) {
	data := resp.GetData()
	switch resp.GetEncoding() {
	case pb.Encoding_ENC_NONE:
		return data, nil
	case pb.Encoding_ENC_ZSTD:
		size := resp.GetUncompressedSize()
		if size > maxDecodedEntry {
			return nil, fmt.Errorf("zstd entry claims %d bytes", size)
		}
		out, err := zstdDecoder.DecodeAll(data, make([]byte, 0, size))
		if err != nil {
			return nil, err
		}
		if uint64(len(out)) != size {
			return nil, fmt.Errorf("zstd entry decoded to %d bytes, not %d", len(out), size)
		}
		return out, nil
	default:
		return nil, errors.New("unknown entry encoding: " + resp.GetEncoding().String())
	}
}
