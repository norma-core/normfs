package normfs_go

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
	pb "github.com/norma-core/normfs/normfs_go/pb/normfs"
)

func response(t *testing.T, resp *pb.ReadResponse) *pb.ReadResponseReader {
	t.Helper()
	reader := pb.NewReadResponseReader()
	if err := reader.Unmarshal(resp.Marshal()); err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestDecodeEntryZstd(t *testing.T) {
	data := bytes.Repeat([]byte("pid=1 user=root status=sleeping\n"), 512)
	enc, _ := zstd.NewWriter(nil)
	packed := enc.EncodeAll(data, nil)

	got, err := decodeEntry(response(t, &pb.ReadResponse{
		Data:             packed,
		Encoding:         pb.Encoding_ENC_ZSTD,
		UncompressedSize: uint64(len(data)),
	}))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("decoded %d bytes, err %v", len(got), err)
	}

	if _, err := decodeEntry(response(t, &pb.ReadResponse{
		Data:             packed,
		Encoding:         pb.Encoding_ENC_ZSTD,
		UncompressedSize: uint64(len(data)) + 1,
	})); err == nil {
		t.Fatal("a size that does not match was accepted")
	}
}

func TestDecodeEntryRaw(t *testing.T) {
	data := []byte("raw")
	got, err := decodeEntry(response(t, &pb.ReadResponse{Data: data}))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("got %q, err %v", got, err)
	}
}
