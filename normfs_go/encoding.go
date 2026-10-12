package normfs_go

import (
	"errors"

	"github.com/klauspost/compress/zstd"
	pb "github.com/norma-core/normfs/normfs_go/pb/normfs"
)

// Sent with every read: the server compresses only queues stored with zstd.
var acceptEncodings = []pb.Encoding{pb.Encoding_ENC_ZSTD}

var zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

func decodeEntry(resp *pb.ReadResponseReader) ([]byte, error) {
	data := resp.GetData()
	switch resp.GetEncoding() {
	case pb.Encoding_ENC_NONE:
		return data, nil
	case pb.Encoding_ENC_ZSTD:
		return zstdDecoder.DecodeAll(data, nil)
	default:
		return nil, errors.New("unknown entry encoding: " + resp.GetEncoding().String())
	}
}
