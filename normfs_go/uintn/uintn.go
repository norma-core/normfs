// Package uintn provides a flexible unsigned integer type system that can represent u8, u16, u32, u64, or arbitrary precision values.
//
// This package provides a unified interface for working with unsigned integers of different sizes,
// with automatic size selection, arithmetic operations, and serialization support.
package uintn

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
)

// UintNType represents the type of the UintN value
type UintNType uint8

const (
	TypeU8 UintNType = iota
	TypeU16
	TypeU32
	TypeU64
	TypeBig
)

// IsValidDataSize checks if a given size is valid for UintN data
func IsValidDataSize(size uint64) bool {
	switch size {
	case 1, 2, 4, 8, 16:
		return true
	default:
		return false
	}
}

// UintN is an interface for flexible unsigned integers
type UintN interface {
	// GetType returns the type of this UintN value
	GetType() UintNType
	// ByteLen returns the byte length of this value
	ByteLen() int
	// ToLEBytes converts to little-endian bytes
	ToLEBytes() []byte
	// ToBEBytes converts to big-endian bytes
	ToBEBytes() []byte
	// ToU64 converts to u64, returning an error if value > MaxUint64
	ToU64() (uint64, error)
	// ToBigInt converts to *big.Int
	ToBigInt() *big.Int
	// IsZero checks if this value is zero
	IsZero() bool
	// String returns string representation
	String() string
	// Equal checks equality with another UintN
	Equal(other UintN) bool
	// Less checks if this value is less than another
	Less(other UintN) bool
	// Greater checks if this value is greater than another
	Greater(other UintN) bool
	// Write writes the value with a length prefix to a bytes.Buffer
	Write(buf *bytes.Buffer) error
	WriteValue(buf *bytes.Buffer) error // WriteValue writes the value without length prefix
	// Add adds another UintN to this value and returns the result
	Add(other UintN) (UintN, error)
}

// Error types for UintN operations
var (
	ErrBufferTooSmall         = errors.New("buffer too small")
	ErrInvalidLengthValue     = errors.New("decoded length doesn't match a valid UintN size")
	ErrSubtractionUnderflow   = errors.New("subtraction resulted in underflow")
	ErrInvalidSizeProvided    = errors.New("size provided is not a valid UintN size")
	ErrInvalidTargetSize      = errors.New("invalid target size for UintN creation")
	ErrValueTooLargeForTarget = errors.New("value too large for target UintN type")
	ErrIncrementOverflow      = errors.New("increment overflowed max value")
	ErrDecrementUnderflow     = errors.New("decrement resulted in underflow")
)

// ErrInvalidByteSliceLength is returned when byte slice has invalid length
type ErrInvalidByteSliceLength struct {
	Got int
}

func (e ErrInvalidByteSliceLength) Error() string {
	return fmt.Sprintf("invalid byte slice length for UintN conversion, must be 1, 2, 4, 8, or 16, got %d", e.Got)
}

// U8 represents an 8-bit unsigned integer
type U8 uint8

func (u U8) GetType() UintNType     { return TypeU8 }
func (u U8) ByteLen() int           { return 1 }
func (u U8) ToLEBytes() []byte      { return []byte{uint8(u)} }
func (u U8) ToBEBytes() []byte      { return []byte{uint8(u)} }
func (u U8) ToU64() (uint64, error) { return uint64(u), nil }
func (u U8) ToBigInt() *big.Int     { return big.NewInt(int64(u)) }
func (u U8) IsZero() bool           { return u == 0 }
func (u U8) String() string         { return fmt.Sprintf("%x", uint8(u)) }

func (u U8) Equal(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return u == v
	case U16:
		return uint16(u) == uint16(v)
	case U32:
		return uint32(u) == uint32(v)
	case U64:
		return uint64(u) == uint64(v)
	case *UintBig:
		return v.Equal(u)
	default:
		return false
	}
}

func (u U8) Less(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return u < v
	case U16, U32, U64, *UintBig:
		return true // u8 is always less than larger types with non-zero values
	default:
		return false
	}
}

func (u U8) Greater(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return u > v
	case U16, U32, U64, *UintBig:
		return false // u8 is never greater than larger types
	default:
		return false
	}
}

// Write writes the value with a length prefix to a bytes.Buffer
func (u U8) Write(buf *bytes.Buffer) error {
	// Write length prefix (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(1)); err != nil {
		return err
	}
	// Write the value
	return buf.WriteByte(uint8(u))
}

// WriteValue writes the value without a length prefix to a bytes.Buffer
func (u U8) WriteValue(buf *bytes.Buffer) error {
	return buf.WriteByte(uint8(u))
}

// Add adds another UintN to this value and returns the result
func (u U8) Add(other UintN) (UintN, error) {
	// Use the existing Add function
	return Add(u, other)
}

// U16 represents a 16-bit unsigned integer
type U16 uint16

func (u U16) GetType() UintNType { return TypeU16 }
func (u U16) ByteLen() int       { return 2 }
func (u U16) ToLEBytes() []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(u))
	return buf
}
func (u U16) ToBEBytes() []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(u))
	return buf
}
func (u U16) ToU64() (uint64, error) { return uint64(u), nil }
func (u U16) ToBigInt() *big.Int     { return big.NewInt(int64(u)) }
func (u U16) IsZero() bool           { return u == 0 }
func (u U16) String() string         { return fmt.Sprintf("%d", uint16(u)) }

func (u U16) Equal(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint16(u) == uint16(v)
	case U16:
		return u == v
	case U32:
		return uint32(u) == uint32(v)
	case U64:
		return uint64(u) == uint64(v)
	case *UintBig:
		return v.Equal(u)
	default:
		return false
	}
}

func (u U16) Less(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint16(u) < uint16(v)
	case U16:
		return u < v
	case U32, U64, *UintBig:
		return uint64(u) < ToU64Must(other)
	default:
		return false
	}
}

func (u U16) Greater(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint16(u) > uint16(v)
	case U16:
		return u > v
	case U32, U64, *UintBig:
		return false
	default:
		return false
	}
}

// Write writes the value with a length prefix to a bytes.Buffer
func (u U16) Write(buf *bytes.Buffer) error {
	// Write length prefix (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(2)); err != nil {
		return err
	}
	// Write the value
	return binary.Write(buf, binary.LittleEndian, uint16(u))
}

// WriteValue writes the value without a length prefix to a bytes.Buffer
func (u U16) WriteValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, uint16(u))
}

// Add adds another UintN to this value and returns the result
func (u U16) Add(other UintN) (UintN, error) {
	return Add(u, other)
}

// U32 represents a 32-bit unsigned integer
type U32 uint32

func (u U32) GetType() UintNType { return TypeU32 }
func (u U32) ByteLen() int       { return 4 }
func (u U32) ToLEBytes() []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(u))
	return buf
}
func (u U32) ToBEBytes() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(u))
	return buf
}
func (u U32) ToU64() (uint64, error) { return uint64(u), nil }
func (u U32) ToBigInt() *big.Int     { return big.NewInt(int64(u)) }
func (u U32) IsZero() bool           { return u == 0 }
func (u U32) String() string         { return fmt.Sprintf("%d", uint32(u)) }

func (u U32) Equal(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint32(u) == uint32(v)
	case U16:
		return uint32(u) == uint32(v)
	case U32:
		return u == v
	case U64:
		return uint64(u) == uint64(v)
	case *UintBig:
		return v.Equal(u)
	default:
		return false
	}
}

func (u U32) Less(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint32(u) < uint32(v)
	case U16:
		return uint32(u) < uint32(v)
	case U32:
		return u < v
	case U64, *UintBig:
		return uint64(u) < ToU64Must(other)
	default:
		return false
	}
}

func (u U32) Greater(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint32(u) > uint32(v)
	case U16:
		return uint32(u) > uint32(v)
	case U32:
		return u > v
	case U64, *UintBig:
		return false
	default:
		return false
	}
}

// Write writes the value with a length prefix to a bytes.Buffer
func (u U32) Write(buf *bytes.Buffer) error {
	// Write length prefix (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(4)); err != nil {
		return err
	}
	// Write the value
	return binary.Write(buf, binary.LittleEndian, uint32(u))
}

func (u U32) WriteValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, uint32(u))
}

// Add adds another UintN to this value and returns the result
func (u U32) Add(other UintN) (UintN, error) {
	return Add(u, other)
}

// U64 represents a 64-bit unsigned integer
type U64 uint64

func (u U64) GetType() UintNType { return TypeU64 }
func (u U64) ByteLen() int       { return 8 }
func (u U64) ToLEBytes() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(u))
	return buf
}
func (u U64) ToBEBytes() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(u))
	return buf
}
func (u U64) ToU64() (uint64, error) { return uint64(u), nil }
func (u U64) ToBigInt() *big.Int {
	b := new(big.Int)
	b.SetUint64(uint64(u))
	return b
}
func (u U64) IsZero() bool   { return u == 0 }
func (u U64) String() string { return fmt.Sprintf("%d", uint64(u)) }

func (u U64) Equal(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint64(u) == uint64(v)
	case U16:
		return uint64(u) == uint64(v)
	case U32:
		return uint64(u) == uint64(v)
	case U64:
		return u == v
	case *UintBig:
		return v.Equal(u)
	default:
		return false
	}
}

func (u U64) Less(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint64(u) < uint64(v)
	case U16:
		return uint64(u) < uint64(v)
	case U32:
		return uint64(u) < uint64(v)
	case U64:
		return u < v
	case *UintBig:
		return v.Greater(u)
	default:
		return false
	}
}

func (u U64) Greater(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return uint64(u) > uint64(v)
	case U16:
		return uint64(u) > uint64(v)
	case U32:
		return uint64(u) > uint64(v)
	case U64:
		return u > v
	case *UintBig:
		return v.Less(u)
	default:
		return false
	}
}

// Write writes the value with a length prefix to a bytes.Buffer
func (u U64) Write(buf *bytes.Buffer) error {
	// Write length prefix (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(8)); err != nil {
		return err
	}
	// Write the value
	return binary.Write(buf, binary.LittleEndian, uint64(u))
}

func (u U64) WriteValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, uint64(u))
}

// Add adds another UintN to this value and returns the result
func (u U64) Add(other UintN) (UintN, error) {
	return Add(u, other)
}

// UintBig represents an arbitrary precision unsigned integer
type UintBig struct {
	v big.Int
}

func (u *UintBig) GetType() UintNType { return TypeBig }
func (u *UintBig) ByteLen() int       { return 16 } // Standard size for serialization
func (u *UintBig) ToLEBytes() []byte {
	bytes := u.v.Bytes()
	if len(bytes) == 0 {
		return make([]byte, 16)
	}

	// Convert big-endian to little-endian
	result := make([]byte, 16)
	for i := 0; i < len(bytes) && i < 16; i++ {
		result[i] = bytes[len(bytes)-1-i]
	}
	return result
}
func (u *UintBig) ToBEBytes() []byte {
	bytes := u.v.Bytes()
	if len(bytes) == 0 {
		return make([]byte, 16)
	}

	// Pad to 16 bytes
	if len(bytes) >= 16 {
		return bytes[:16]
	}

	result := make([]byte, 16)
	copy(result[16-len(bytes):], bytes)
	return result
}
func (u *UintBig) ToU64() (uint64, error) {
	if u.v.IsUint64() {
		return u.v.Uint64(), nil
	}
	return 0, ErrValueTooLargeForTarget
}
func (u *UintBig) ToBigInt() *big.Int { return &u.v }
func (u *UintBig) IsZero() bool       { return u.v.Sign() == 0 }
func (u *UintBig) String() string     { return u.v.String() }

func (u *UintBig) Equal(other UintN) bool {
	switch v := other.(type) {
	case U8:
		return u.v.IsUint64() && u.v.Uint64() == uint64(v)
	case U16:
		return u.v.IsUint64() && u.v.Uint64() == uint64(v)
	case U32:
		return u.v.IsUint64() && u.v.Uint64() == uint64(v)
	case U64:
		return u.v.IsUint64() && u.v.Uint64() == uint64(v)
	case *UintBig:
		return u.v.Cmp(&v.v) == 0
	default:
		return false
	}
}

func (u *UintBig) Less(other UintN) bool {
	switch v := other.(type) {
	case *UintBig:
		return u.v.Cmp(&v.v) < 0
	default:
		otherBig := other.ToBigInt()
		return u.v.Cmp(otherBig) < 0
	}
}

func (u *UintBig) Greater(other UintN) bool {
	switch v := other.(type) {
	case *UintBig:
		return u.v.Cmp(&v.v) > 0
	default:
		otherBig := other.ToBigInt()
		return u.v.Cmp(otherBig) > 0
	}
}

// Write writes the value with a length prefix to a bytes.Buffer
func (u *UintBig) Write(buf *bytes.Buffer) error {
	// Write length prefix (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint64(16)); err != nil {
		return err
	}
	// Write the value (16 bytes, little-endian)
	leBytes := u.ToLEBytes()
	_, err := buf.Write(leBytes)
	return err
}

func (u *UintBig) WriteValue(buf *bytes.Buffer) error {
	leBytes := u.ToLEBytes()
	_, err := buf.Write(leBytes)

	return err
}

// Add adds another UintN to this value and returns the result
func (u *UintBig) Add(other UintN) (UintN, error) {
	return Add(u, other)
}

// ToU64Must gets u64 value or panics
func ToU64Must(u UintN) uint64 {
	v, err := u.ToU64()
	if err != nil {
		panic(err)
	}
	return v
}

// Constructor functions

// FromU8 creates a UintN from u8
func FromU8(value uint8) UintN {
	return U8(value)
}

// FromU16 creates a UintN from u16, automatically selecting the smallest appropriate type
func FromU16(value uint16) UintN {
	if value <= 255 {
		return U8(value)
	}
	return U16(value)
}

// FromU32 creates a UintN from u32, automatically selecting the smallest appropriate type
func FromU32(value uint32) UintN {
	if value <= 255 {
		return U8(value)
	}
	if value <= 65535 {
		return U16(value)
	}
	return U32(value)
}

// FromU64 creates a UintN from u64, automatically selecting the smallest appropriate type
func FromU64(value uint64) UintN {
	if value <= 255 {
		return U8(value)
	}
	if value <= 65535 {
		return U16(value)
	}
	if value <= 4294967295 {
		return U32(value)
	}
	return U64(value)
}

// FromBigInt creates a UintN from *big.Int, automatically selecting the smallest appropriate type
func FromBigInt(value *big.Int) UintN {
	if value.IsUint64() {
		return FromU64(value.Uint64())
	}

	result := &UintBig{}
	result.v.Set(value)
	return result
}

// FromU64WithKnownSize creates a UintN from u64 with a known target size
func FromU64WithKnownSize(value uint64, sizeInBytes int) (UintN, error) {
	switch sizeInBytes {
	case 1:
		if value > 255 {
			return nil, ErrValueTooLargeForTarget
		}
		return U8(value), nil
	case 2:
		if value > 65535 {
			return nil, ErrValueTooLargeForTarget
		}
		return U16(value), nil
	case 4:
		if value > 4294967295 {
			return nil, ErrValueTooLargeForTarget
		}
		return U32(value), nil
	case 8:
		return U64(value), nil
	case 16:
		b := new(big.Int)
		b.SetUint64(value)
		return &UintBig{v: *b}, nil
	default:
		return nil, ErrInvalidTargetSize
	}
}

// FromBigIntWithKnownSize creates a UintN from *big.Int with a known target size
func FromBigIntWithKnownSize(value *big.Int, sizeInBytes int) (UintN, error) {
	switch sizeInBytes {
	case 1:
		if !value.IsUint64() || value.Uint64() > 255 {
			return nil, ErrValueTooLargeForTarget
		}
		return U8(value.Uint64()), nil
	case 2:
		if !value.IsUint64() || value.Uint64() > 65535 {
			return nil, ErrValueTooLargeForTarget
		}
		return U16(value.Uint64()), nil
	case 4:
		if !value.IsUint64() || value.Uint64() > 4294967295 {
			return nil, ErrValueTooLargeForTarget
		}
		return U32(value.Uint64()), nil
	case 8:
		if !value.IsUint64() {
			return nil, ErrValueTooLargeForTarget
		}
		return U64(value.Uint64()), nil
	case 16:
		result := &UintBig{}
		result.v.Set(value)
		return result, nil
	default:
		return nil, ErrInvalidTargetSize
	}
}

// FromLEBytes creates a UintN from little-endian bytes
func FromLEBytes(data []byte) (UintN, error) {
	switch len(data) {
	case 1:
		return U8(data[0]), nil
	case 2:
		return U16(binary.LittleEndian.Uint16(data)), nil
	case 4:
		return U32(binary.LittleEndian.Uint32(data)), nil
	case 8:
		return U64(binary.LittleEndian.Uint64(data)), nil
	case 16:
		// Convert little-endian to big-endian for big.Int
		reversed := make([]byte, 16)
		for i := 0; i < 16; i++ {
			reversed[i] = data[15-i]
		}
		b := new(big.Int)
		b.SetBytes(reversed)
		return &UintBig{v: *b}, nil
	default:
		return nil, ErrInvalidByteSliceLength{Got: len(data)}
	}
}

// FromBEBytes creates a UintN from big-endian bytes
func FromBEBytes(data []byte) (UintN, error) {
	if len(data) == 0 {
		return nil, ErrInvalidSizeProvided
	}
	if len(data) > 16 {
		return nil, ErrInvalidByteSliceLength{Got: len(data)}
	}

	// Convert big-endian byte slice to big.Int
	b := new(big.Int)
	b.SetBytes(data)
	return FromBigInt(b), nil
}

// Arithmetic operations

func ShouldInc(u UintN) UintN {
	res, err := Inc(u)
	if err != nil {
		panic(err)
	}
	return res
}

func ShouldDec(u UintN) UintN {
	res, err := Dec(u)
	if err != nil {
		panic(err)
	}
	return res
}

// Inc increments the value, potentially growing to a larger type
func Inc(u UintN) (UintN, error) {
	switch v := u.(type) {
	case U8:
		if v == 255 {
			return U16(256), nil
		}
		return U8(v + 1), nil
	case U16:
		if v == 65535 {
			return U32(65536), nil
		}
		return U16(v + 1), nil
	case U32:
		if v == 4294967295 {
			return U64(4294967296), nil
		}
		return U32(v + 1), nil
	case U64:
		if v == ^U64(0) { // Max uint64
			b := new(big.Int)
			b.SetUint64(uint64(v))
			b.Add(b, big.NewInt(1))
			return &UintBig{v: *b}, nil
		}
		return U64(v + 1), nil
	case *UintBig:
		result := &UintBig{}
		result.v.Add(&v.v, big.NewInt(1))
		return result, nil
	default:
		return nil, ErrInvalidTargetSize
	}
}

// Dec decrements the value
func Dec(u UintN) (UintN, error) {
	if u.IsZero() {
		return nil, ErrDecrementUnderflow
	}

	switch v := u.(type) {
	case U8:
		return U8(v - 1), nil
	case U16:
		return FromU16(uint16(v) - 1), nil
	case U32:
		return FromU32(uint32(v) - 1), nil
	case U64:
		return FromU64(uint64(v) - 1), nil
	case *UintBig:
		result := &UintBig{}
		result.v.Sub(&v.v, big.NewInt(1))
		return FromBigInt(&result.v), nil
	default:
		return nil, ErrInvalidTargetSize
	}
}

// Add adds two UintN values, automatically selecting the appropriate result type
func Add(a, b UintN) (UintN, error) {
	if b.IsZero() {
		return a, nil
	}

	// Try to keep operations in native types as long as possible
	aU64, aErr := a.ToU64()
	bU64, bErr := b.ToU64()

	if aErr == nil && bErr == nil {
		// Both fit in u64
		if aU64 <= ^uint64(0)-bU64 { // Check for overflow
			return FromU64(aU64 + bU64), nil
		}
		// Overflow to big.Int
		result := &UintBig{}
		result.v.SetUint64(aU64)
		result.v.Add(&result.v, new(big.Int).SetUint64(bU64))
		return result, nil
	}

	// At least one is big.Int
	aBig := a.ToBigInt()
	bBig := b.ToBigInt()
	result := &UintBig{}
	result.v.Add(aBig, bBig)
	return result, nil
}

// Sub subtracts two UintN values
func Sub(a, b UintN) (UintN, error) {
	if b.IsZero() {
		return a, nil
	}

	// Check for underflow
	if a.Less(b) {
		return nil, ErrSubtractionUnderflow
	}

	// Try to keep operations in native types
	aU64, aErr := a.ToU64()
	bU64, bErr := b.ToU64()

	if aErr == nil && bErr == nil {
		return FromU64(aU64 - bU64), nil
	}

	// At least one is big.Int
	aBig := a.ToBigInt()
	bBig := b.ToBigInt()
	result := &UintBig{}
	result.v.Sub(aBig, bBig)
	return FromBigInt(&result.v), nil
}

// ShrinkToFit returns the smallest type that can hold this value
func ShrinkToFit(u UintN) UintN {
	switch v := u.(type) {
	case U8:
		return v
	case U16:
		return FromU16(uint16(v))
	case U32:
		return FromU32(uint32(v))
	case U64:
		return FromU64(uint64(v))
	case *UintBig:
		return FromBigInt(&v.v)
	default:
		return u
	}
}

// Progress calculates the progress of 'current' between 'from' and 'to' as a float64 [0.0, 1.0]
func Progress(current, from, to UintN) float64 {
	if current.Less(from) || current.Equal(from) {
		return 0.0
	}
	if current.Greater(to) || current.Equal(to) {
		return 1.0
	}

	numerator, err := Sub(current, from)
	if err != nil {
		return 0.0
	}

	denominator, err := Sub(to, from)
	if err != nil {
		return 0.0
	}

	if denominator.IsZero() {
		return 1.0
	}

	numFloat := new(big.Float).SetInt(numerator.ToBigInt())
	denFloat := new(big.Float).SetInt(denominator.ToBigInt())

	result := new(big.Float).Quo(numFloat, denFloat)
	f, _ := result.Float64()
	return f
}

// Resize resizes to a specific byte size
func Resize(u UintN, sizeInBytes int) (UintN, error) {
	switch v := u.(type) {
	case U8:
		return FromU64WithKnownSize(uint64(v), sizeInBytes)
	case U16:
		return FromU64WithKnownSize(uint64(v), sizeInBytes)
	case U32:
		return FromU64WithKnownSize(uint64(v), sizeInBytes)
	case U64:
		return FromU64WithKnownSize(uint64(v), sizeInBytes)
	case *UintBig:
		return FromBigIntWithKnownSize(&v.v, sizeInBytes)
	default:
		return nil, ErrInvalidTargetSize
	}
}

// WriteValue writes the value to a target slice
func WriteValue(u UintN, target []byte) error {
	valBytes := u.ToLEBytes()
	if len(target) < len(valBytes) {
		return ErrBufferTooSmall
	}
	copy(target[:len(valBytes)], valBytes)
	return nil
}

// Write writes the value with a length prefix (u64 little-endian)
func Write(u UintN, target []byte) error {
	valLen := u.ByteLen()

	if len(target) < 8 {
		return ErrBufferTooSmall
	}

	// Write length prefix
	binary.LittleEndian.PutUint64(target[:8], uint64(valLen))

	if len(target) < 8+valLen {
		return ErrBufferTooSmall
	}

	// Write value
	return WriteValue(u, target[8:8+valLen])
}

// ReadValueFromSlice reads a value from a slice with a known size
func ReadValueFromSlice(slice []byte, size int) (UintN, error) {
	if len(slice) < size {
		return nil, ErrBufferTooSmall
	}

	return FromLEBytes(slice[:size])
}

// ReadFromSlice reads from a slice with a length prefix
func ReadFromSlice(slice []byte) (UintN, int, error) {
	if len(slice) < 8 {
		return nil, 0, ErrBufferTooSmall
	}

	valueLen := int(binary.LittleEndian.Uint64(slice[:8]))

	if !IsValidDataSize(uint64(valueLen)) {
		return nil, 0, ErrInvalidLengthValue
	}

	totalLen := 8 + valueLen
	if len(slice) < totalLen {
		return nil, 0, ErrBufferTooSmall
	}

	value, err := ReadValueFromSlice(slice[8:totalLen], valueLen)
	if err != nil {
		return nil, 0, err
	}

	return value, totalLen, nil
}

// DivideByTwo divides a UintN by 2 using right shift
func DivideByTwo(u UintN) UintN {
	switch v := u.(type) {
	case U8:
		return U8(uint8(v) / 2)
	case U16:
		return U16(uint16(v) / 2)
	case U32:
		return U32(uint32(v) / 2)
	case U64:
		return U64(uint64(v) / 2)
	case *UintBig:
		result := &UintBig{}
		result.v.Rsh(&v.v, 1) // Right shift by 1 (divide by 2)
		return result
	default:
		return u
	}
}

// Middle calculates the midpoint between two UintN values: (a + b) / 2
func Middle(a, b UintN) UintN {
	// Calculate (a + b) / 2
	sum, err := Add(a, b)
	if err != nil {
		// Overflow case - use a safer approach: a + (b - a) / 2
		diff, err := Sub(b, a)
		if err != nil {
			return a // Fallback
		}
		half := DivideByTwo(diff)
		result, err := Add(a, half)
		if err != nil {
			return a // Fallback
		}
		return result
	}

	return DivideByTwo(sum)
}

// ParseDecimal parses a decimal string into a UintN, automatically selecting
// the smallest type that can hold the value
func ParseDecimal(s string) (UintN, error) {
	// Parse as big.Int to handle arbitrarily large numbers
	b := new(big.Int)
	_, ok := b.SetString(s, 10) // base 10
	if !ok {
		return nil, fmt.Errorf("invalid decimal string: %s", s)
	}

	// Check for negative numbers
	if b.Sign() < 0 {
		return nil, fmt.Errorf("negative numbers not supported: %s", s)
	}

	// FromBigInt automatically selects smallest type
	return FromBigInt(b), nil
}
