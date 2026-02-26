package uintn

import (
	"bytes"
	"math/big"
	"testing"
)

func TestBasicTypes(t *testing.T) {
	// Test U8
	u8 := FromU8(255)
	if u8.GetType() != TypeU8 {
		t.Errorf("Expected TypeU8, got %v", u8.GetType())
	}
	if u8.ByteLen() != 1 {
		t.Errorf("Expected ByteLen 1, got %d", u8.ByteLen())
	}
	if !u8.Equal(U8(255)) {
		t.Errorf("Expected equal to 255")
	}

	// Test U16
	u16 := FromU16(65535)
	if u16.GetType() != TypeU16 {
		t.Errorf("Expected TypeU16, got %v", u16.GetType())
	}
	if u16.ByteLen() != 2 {
		t.Errorf("Expected ByteLen 2, got %d", u16.ByteLen())
	}

	// Test U32
	u32 := FromU32(4294967295)
	if u32.GetType() != TypeU32 {
		t.Errorf("Expected TypeU32, got %v", u32.GetType())
	}
	if u32.ByteLen() != 4 {
		t.Errorf("Expected ByteLen 4, got %d", u32.ByteLen())
	}

	// Test U64
	u64 := FromU64(^uint64(0))
	if u64.GetType() != TypeU64 {
		t.Errorf("Expected TypeU64, got %v", u64.GetType())
	}
	if u64.ByteLen() != 8 {
		t.Errorf("Expected ByteLen 8, got %d", u64.ByteLen())
	}
}

func TestAutoShrink(t *testing.T) {
	// Test that FromU16 shrinks to U8 when possible
	u := FromU16(100)
	if u.GetType() != TypeU8 {
		t.Errorf("Expected TypeU8 for value 100, got %v", u.GetType())
	}

	// Test that FromU32 shrinks appropriately
	u = FromU32(100)
	if u.GetType() != TypeU8 {
		t.Errorf("Expected TypeU8 for value 100, got %v", u.GetType())
	}

	u = FromU32(1000)
	if u.GetType() != TypeU16 {
		t.Errorf("Expected TypeU16 for value 1000, got %v", u.GetType())
	}

	u = FromU32(70000)
	if u.GetType() != TypeU32 {
		t.Errorf("Expected TypeU32 for value 70000, got %v", u.GetType())
	}

	// Test that FromU64 shrinks appropriately
	u = FromU64(255)
	if u.GetType() != TypeU8 {
		t.Errorf("Expected TypeU8 for value 255, got %v", u.GetType())
	}
}

func TestIncrement(t *testing.T) {
	// Test U8 increment without overflow
	u, err := Inc(U8(100))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !u.Equal(U8(101)) {
		t.Errorf("Expected 101, got %v", u)
	}

	// Test U8 increment with overflow to U16
	u, err = Inc(U8(255))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if u.GetType() != TypeU16 || !u.Equal(U16(256)) {
		t.Errorf("Expected U16(256), got %v", u)
	}

	// Test U64 increment with overflow to BigInt
	maxU64 := U64(^uint64(0))
	u, err = Inc(maxU64)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if u.GetType() != TypeBig {
		t.Errorf("Expected TypeBig, got %v", u.GetType())
	}
	expected := new(big.Int).SetUint64(^uint64(0))
	expected.Add(expected, big.NewInt(1))
	if u.ToBigInt().Cmp(expected) != 0 {
		t.Errorf("Expected %v, got %v", expected, u.ToBigInt())
	}
}

func TestDecrement(t *testing.T) {
	// Test normal decrement
	u, err := Dec(U8(100))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !u.Equal(U8(99)) {
		t.Errorf("Expected 99, got %v", u)
	}

	// Test decrement with shrinking
	u, err = Dec(U16(256))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if u.GetType() != TypeU8 || !u.Equal(U8(255)) {
		t.Errorf("Expected U8(255), got %v", u)
	}

	// Test underflow error
	_, err = Dec(U8(0))
	if err != ErrDecrementUnderflow {
		t.Errorf("Expected ErrDecrementUnderflow, got %v", err)
	}
}

func TestAddition(t *testing.T) {
	// Test simple addition
	a := U8(100)
	b := U8(50)
	result, err := Add(a, b)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !result.Equal(U8(150)) {
		t.Errorf("Expected 150, got %v", result)
	}

	// Test addition with overflow
	a = U8(200)
	b = U8(100)
	result, err = Add(a, b)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.GetType() != TypeU16 || !result.Equal(U16(300)) {
		t.Errorf("Expected U16(300), got %v", result)
	}

	// Test mixed type addition
	a2 := U16(1000)
	b2 := U32(2000)
	result, err = Add(a2, b2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !result.Equal(U32(3000)) {
		t.Errorf("Expected 3000, got %v", result)
	}

	// Test addition with zero
	result, err = Add(U32(100), U8(0))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !result.Equal(U32(100)) {
		t.Errorf("Expected 100, got %v", result)
	}
}

func TestSubtraction(t *testing.T) {
	// Test simple subtraction
	a := U8(100)
	b := U8(50)
	result, err := Sub(a, b)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !result.Equal(U8(50)) {
		t.Errorf("Expected 50, got %v", result)
	}

	// Test subtraction with shrinking
	a2 := U32(256)
	b2 := U32(1)
	result, err = Sub(a2, b2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.GetType() != TypeU8 || !result.Equal(U8(255)) {
		t.Errorf("Expected U8(255), got %v", result)
	}

	// Test underflow error
	_, err = Sub(U8(50), U8(100))
	if err != ErrSubtractionUnderflow {
		t.Errorf("Expected ErrSubtractionUnderflow, got %v", err)
	}

	// Test subtraction with zero
	result, err = Sub(U32(100), U8(0))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !result.Equal(U32(100)) {
		t.Errorf("Expected 100, got %v", result)
	}
}

func TestComparison(t *testing.T) {
	// Test equality
	if !U8(100).Equal(U8(100)) {
		t.Errorf("Expected U8(100) == U8(100)")
	}
	if !U8(100).Equal(U16(100)) {
		t.Errorf("Expected U8(100) == U16(100)")
	}
	if U8(100).Equal(U8(101)) {
		t.Errorf("Expected U8(100) != U8(101)")
	}

	// Test less than
	if !U8(50).Less(U8(100)) {
		t.Errorf("Expected U8(50) < U8(100)")
	}
	if !U8(255).Less(U16(256)) {
		t.Errorf("Expected U8(255) < U16(256)")
	}
	if U8(100).Less(U8(50)) {
		t.Errorf("Expected U8(100) >= U8(50)")
	}

	// Test greater than
	if !U8(100).Greater(U8(50)) {
		t.Errorf("Expected U8(100) > U8(50)")
	}
	if U8(100).Greater(U16(200)) {
		t.Errorf("Expected U8(100) <= U16(200)")
	}
}

func TestSerialization(t *testing.T) {
	// Test ToLEBytes
	u := U32(0x12345678)
	leBytes := u.ToLEBytes()
	expected := []byte{0x78, 0x56, 0x34, 0x12}
	if !bytes.Equal(leBytes, expected) {
		t.Errorf("Expected %v, got %v", expected, leBytes)
	}

	// Test ToBEBytes
	beBytes := u.ToBEBytes()
	expected = []byte{0x12, 0x34, 0x56, 0x78}
	if !bytes.Equal(beBytes, expected) {
		t.Errorf("Expected %v, got %v", expected, beBytes)
	}

	// Test FromLEBytes
	u2, err := FromLEBytes([]byte{0x78, 0x56, 0x34, 0x12})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !u2.Equal(U32(0x12345678)) {
		t.Errorf("Expected 0x12345678, got %v", u2)
	}

	// Test FromBEBytes
	u3, err := FromBEBytes([]byte{0x12, 0x34, 0x56, 0x78})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !u3.Equal(U32(0x12345678)) {
		t.Errorf("Expected 0x12345678, got %v", u3)
	}
}

func TestWriteRead(t *testing.T) {
	// Test Write and ReadFromSlice
	u := U32(12345)
	buf := make([]byte, 20)

	err := Write(u, buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	u2, bytesRead, err := ReadFromSlice(buf)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if bytesRead != 12 { // 8 bytes for length + 4 bytes for U32
		t.Errorf("Expected 12 bytes read, got %d", bytesRead)
	}
	if !u2.Equal(u) {
		t.Errorf("Expected %v, got %v", u, u2)
	}
}

func TestResize(t *testing.T) {
	// Test resizing up
	u := U8(100)
	resized, err := Resize(u, 4)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if resized.GetType() != TypeU32 || !resized.Equal(U32(100)) {
		t.Errorf("Expected U32(100), got %v", resized)
	}

	// Test resizing down
	u2 := U32(100)
	resized, err = Resize(u2, 1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if resized.GetType() != TypeU8 || !resized.Equal(U8(100)) {
		t.Errorf("Expected U8(100), got %v", resized)
	}

	// Test resize with value too large
	u3 := U32(300)
	_, err = Resize(u3, 1)
	if err != ErrValueTooLargeForTarget {
		t.Errorf("Expected ErrValueTooLargeForTarget, got %v", err)
	}
}

func TestBigInt(t *testing.T) {
	// Test creating UintBig
	b := new(big.Int)
	b.SetString("340282366920938463463374607431768211456", 10) // 2^128
	u := FromBigInt(b)
	if u.GetType() != TypeBig {
		t.Errorf("Expected TypeBig, got %v", u.GetType())
	}

	// Test operations with BigInt
	u2 := U64(1)
	result, err := Add(u, u2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.GetType() != TypeBig {
		t.Errorf("Expected TypeBig, got %v", result.GetType())
	}

	expected := new(big.Int)
	expected.SetString("340282366920938463463374607431768211457", 10)
	if result.ToBigInt().Cmp(expected) != 0 {
		t.Errorf("Expected %v, got %v", expected, result.ToBigInt())
	}
}

func TestIsZero(t *testing.T) {
	if !U8(0).IsZero() {
		t.Errorf("Expected U8(0) to be zero")
	}
	if U8(1).IsZero() {
		t.Errorf("Expected U8(1) to not be zero")
	}
	if !U64(0).IsZero() {
		t.Errorf("Expected U64(0) to be zero")
	}

	b := &UintBig{v: *big.NewInt(0)}
	if !b.IsZero() {
		t.Errorf("Expected UintBig(0) to be zero")
	}
}

func TestInvalidOperations(t *testing.T) {
	// Test invalid byte slice lengths
	_, err := FromLEBytes([]byte{1, 2, 3})
	if _, ok := err.(ErrInvalidByteSliceLength); !ok {
		t.Errorf("Expected ErrInvalidByteSliceLength, got %v", err)
	}

	// Test invalid size for resize
	_, err = Resize(U8(100), 3)
	if err != ErrInvalidTargetSize {
		t.Errorf("Expected ErrInvalidTargetSize, got %v", err)
	}

	// Test buffer too small
	u := U32(12345)
	buf := make([]byte, 2)
	err = Write(u, buf)
	if err != ErrBufferTooSmall {
		t.Errorf("Expected ErrBufferTooSmall, got %v", err)
	}
}

func TestShrinkToFit(t *testing.T) {
	// Test that large values shrink appropriately
	u := U64(100)
	shrunk := ShrinkToFit(u)
	if shrunk.GetType() != TypeU8 {
		t.Errorf("Expected TypeU8, got %v", shrunk.GetType())
	}

	u2 := U32(70000)
	shrunk = ShrinkToFit(u2)
	if shrunk.GetType() != TypeU32 {
		t.Errorf("Expected TypeU32, got %v", shrunk.GetType())
	}
}

func BenchmarkAddNative(b *testing.B) {
	a := U32(1000000)
	c := U32(1)
	for i := 0; i < b.N; i++ {
		_, _ = Add(a, c)
	}
}

func BenchmarkAddBigInt(b *testing.B) {
	// Create a big value that requires BigInt
	big1 := new(big.Int)
	big1.SetString("340282366920938463463374607431768211456", 10)
	a := FromBigInt(big1)
	c := U64(1)

	for i := 0; i < b.N; i++ {
		_, _ = Add(a, c)
	}
}

func BenchmarkIncrement(b *testing.B) {
	var u UintN = U32(1000000)
	for i := 0; i < b.N; i++ {
		u, _ = Inc(u)
	}
}

func BenchmarkSerialization(b *testing.B) {
	u := U64(1234567890)
	buf := make([]byte, 20)

	for i := 0; i < b.N; i++ {
		_ = Write(u, buf)
		_, _, _ = ReadFromSlice(buf)
	}
}
