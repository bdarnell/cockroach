// Code generated by protoc-gen-gogo.
// source: cockroach/rpc/codec/wire/wire.proto
// DO NOT EDIT!

/*
	Package wire is a generated protocol buffer package.

		protorpc wire format wrapper

		0. Frame Format
		len : uvarint64
		data: byte[len]

		1. Client Send Request
		Send RequestHeader: sendFrame(conn, hdr, len(hdr))
		Send Request: sendFrame(conn, body, len(body))

		2. Server Recv Request
		Recv RequestHeader: recvFrame(conn, hdr, max_hdr_len, 0)
		Recv Request: recvFrame(conn, body)

		3. Server Send Response
		Send ResponseHeader: sendFrame(conn, hdr, len(hdr))
		Send Response: sendFrame(conn, body, len(body))

		4. Client Recv Response
		Recv ResponseHeader: recvFrame(conn, hdr, max_hdr_len, 0)
		Recv Response: recvFrame(conn, body)

	It is generated from these files:
		cockroach/rpc/codec/wire/wire.proto

	It has these top-level messages:
		RequestHeader
		ResponseHeader
*/
package wire

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

// discarding unused import gogoproto "github.com/cockroachdb/gogoproto"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type CompressionType int32

const (
	CompressionType_NONE   CompressionType = 0
	CompressionType_SNAPPY CompressionType = 1
	CompressionType_LZ4    CompressionType = 2
)

var CompressionType_name = map[int32]string{
	0: "NONE",
	1: "SNAPPY",
	2: "LZ4",
}
var CompressionType_value = map[string]int32{
	"NONE":   0,
	"SNAPPY": 1,
	"LZ4":    2,
}

func (x CompressionType) Enum() *CompressionType {
	p := new(CompressionType)
	*p = x
	return p
}
func (x CompressionType) String() string {
	return proto.EnumName(CompressionType_name, int32(x))
}
func (x *CompressionType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(CompressionType_value, data, "CompressionType")
	if err != nil {
		return err
	}
	*x = CompressionType(value)
	return nil
}

type RequestHeader struct {
	Id               uint64          `protobuf:"varint,1,opt,name=id" json:"id"`
	Method           *string         `protobuf:"bytes,2,opt,name=method" json:"method,omitempty"`
	MethodId         int32           `protobuf:"varint,3,opt,name=method_id" json:"method_id"`
	Compression      CompressionType `protobuf:"varint,4,opt,name=compression,enum=cockroach.rpc.codec.wire.CompressionType" json:"compression"`
	UncompressedSize uint32          `protobuf:"varint,5,opt,name=uncompressed_size" json:"uncompressed_size"`
}

func (m *RequestHeader) Reset()         { *m = RequestHeader{} }
func (m *RequestHeader) String() string { return proto.CompactTextString(m) }
func (*RequestHeader) ProtoMessage()    {}

type ResponseHeader struct {
	Id               uint64          `protobuf:"varint,1,opt,name=id" json:"id"`
	Method           string          `protobuf:"bytes,2,opt,name=method" json:"method"`
	Error            string          `protobuf:"bytes,3,opt,name=error" json:"error"`
	Compression      CompressionType `protobuf:"varint,4,opt,name=compression,enum=cockroach.rpc.codec.wire.CompressionType" json:"compression"`
	UncompressedSize uint32          `protobuf:"varint,5,opt,name=uncompressed_size" json:"uncompressed_size"`
}

func (m *ResponseHeader) Reset()         { *m = ResponseHeader{} }
func (m *ResponseHeader) String() string { return proto.CompactTextString(m) }
func (*ResponseHeader) ProtoMessage()    {}

func init() {
	proto.RegisterEnum("cockroach.rpc.codec.wire.CompressionType", CompressionType_name, CompressionType_value)
}
func (m *RequestHeader) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *RequestHeader) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0x8
	i++
	i = encodeVarintWire(data, i, uint64(m.Id))
	if m.Method != nil {
		data[i] = 0x12
		i++
		i = encodeVarintWire(data, i, uint64(len(*m.Method)))
		i += copy(data[i:], *m.Method)
	}
	data[i] = 0x18
	i++
	i = encodeVarintWire(data, i, uint64(m.MethodId))
	data[i] = 0x20
	i++
	i = encodeVarintWire(data, i, uint64(m.Compression))
	data[i] = 0x28
	i++
	i = encodeVarintWire(data, i, uint64(m.UncompressedSize))
	return i, nil
}

func (m *ResponseHeader) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *ResponseHeader) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0x8
	i++
	i = encodeVarintWire(data, i, uint64(m.Id))
	data[i] = 0x12
	i++
	i = encodeVarintWire(data, i, uint64(len(m.Method)))
	i += copy(data[i:], m.Method)
	data[i] = 0x1a
	i++
	i = encodeVarintWire(data, i, uint64(len(m.Error)))
	i += copy(data[i:], m.Error)
	data[i] = 0x20
	i++
	i = encodeVarintWire(data, i, uint64(m.Compression))
	data[i] = 0x28
	i++
	i = encodeVarintWire(data, i, uint64(m.UncompressedSize))
	return i, nil
}

func encodeFixed64Wire(data []byte, offset int, v uint64) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	data[offset+4] = uint8(v >> 32)
	data[offset+5] = uint8(v >> 40)
	data[offset+6] = uint8(v >> 48)
	data[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Wire(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintWire(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
func (m *RequestHeader) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovWire(uint64(m.Id))
	if m.Method != nil {
		l = len(*m.Method)
		n += 1 + l + sovWire(uint64(l))
	}
	n += 1 + sovWire(uint64(m.MethodId))
	n += 1 + sovWire(uint64(m.Compression))
	n += 1 + sovWire(uint64(m.UncompressedSize))
	return n
}

func (m *ResponseHeader) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovWire(uint64(m.Id))
	l = len(m.Method)
	n += 1 + l + sovWire(uint64(l))
	l = len(m.Error)
	n += 1 + l + sovWire(uint64(l))
	n += 1 + sovWire(uint64(m.Compression))
	n += 1 + sovWire(uint64(m.UncompressedSize))
	return n
}

func sovWire(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozWire(x uint64) (n int) {
	return sovWire(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RequestHeader) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWire
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RequestHeader: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RequestHeader: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Method", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthWire
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			s := string(data[iNdEx:postIndex])
			m.Method = &s
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MethodId", wireType)
			}
			m.MethodId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.MethodId |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Compression", wireType)
			}
			m.Compression = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Compression |= (CompressionType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field UncompressedSize", wireType)
			}
			m.UncompressedSize = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.UncompressedSize |= (uint32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipWire(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWire
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ResponseHeader) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWire
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ResponseHeader: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ResponseHeader: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Method", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthWire
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Method = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Error", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthWire
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Error = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Compression", wireType)
			}
			m.Compression = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Compression |= (CompressionType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field UncompressedSize", wireType)
			}
			m.UncompressedSize = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.UncompressedSize |= (uint32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipWire(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWire
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipWire(data []byte) (n int, err error) {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowWire
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowWire
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if data[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowWire
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthWire
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowWire
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipWire(data[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthWire = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowWire   = fmt.Errorf("proto: integer overflow")
)
