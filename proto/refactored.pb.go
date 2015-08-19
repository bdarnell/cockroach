// Code generated by protoc-gen-gogo.
// source: cockroach/proto/refactored.proto
// DO NOT EDIT!

package proto

import proto1 "github.com/gogo/protobuf/proto"
import math "math"

// discarding unused import gogoproto "gogoproto"

import io "io"
import fmt "fmt"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto1.Marshal
var _ = math.Inf

// KeyHeader replaces RequestHeader.
type KeyHeader struct {
	Key    Key `protobuf:"bytes,1,opt,name=key,casttype=Key" json:"key,omitempty"`
	EndKey Key `protobuf:"bytes,2,opt,name=end_key,casttype=Key" json:"end_key,omitempty"`
}

func (m *KeyHeader) Reset()         { *m = KeyHeader{} }
func (m *KeyHeader) String() string { return proto1.CompactTextString(m) }
func (*KeyHeader) ProtoMessage()    {}

func (m *KeyHeader) GetKey() Key {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *KeyHeader) GetEndKey() Key {
	if m != nil {
		return m.EndKey
	}
	return nil
}

// NBatch is the only type of request sent by clients. Currently named
// NBatch only since Batch is taken.
type NBatch struct {
	CmdID        ClientCmdID    `protobuf:"bytes,1,opt,name=cmd_id" json:"cmd_id"`
	User         string         `protobuf:"bytes,2,opt,name=user" json:"user"`
	UserPriority *int32         `protobuf:"varint,3,opt,name=user_priority,def=1" json:"user_priority,omitempty"`
	Txn          *Transaction   `protobuf:"bytes,4,opt,name=txn" json:"txn,omitempty"`
	Requests     []RequestUnion `protobuf:"bytes,5,rep,name=requests" json:"requests"`
}

func (m *NBatch) Reset()         { *m = NBatch{} }
func (m *NBatch) String() string { return proto1.CompactTextString(m) }
func (*NBatch) ProtoMessage()    {}

const Default_NBatch_UserPriority int32 = 1

func (m *NBatch) GetCmdID() ClientCmdID {
	if m != nil {
		return m.CmdID
	}
	return ClientCmdID{}
}

func (m *NBatch) GetUser() string {
	if m != nil {
		return m.User
	}
	return ""
}

func (m *NBatch) GetUserPriority() int32 {
	if m != nil && m.UserPriority != nil {
		return *m.UserPriority
	}
	return Default_NBatch_UserPriority
}

func (m *NBatch) GetTxn() *Transaction {
	if m != nil {
		return m.Txn
	}
	return nil
}

func (m *NBatch) GetRequests() []RequestUnion {
	if m != nil {
		return m.Requests
	}
	return nil
}

// A Batch is internally cut into at least one BatchChunk. The criteria are
// * BatchChunk doesn't contain both "standard" and reverse operations.
// * BatchChunk is either completely Raft or completely non-Raft.
// * BatchChunk's requests address a single range (or it needs re-cutting).
type BatchChunk struct {
	Replica Replica `protobuf:"bytes,1,opt,name=replica" json:"replica"`
	RangeID RangeID `protobuf:"varint,2,opt,name=range_id,casttype=RangeID" json:"range_id"`
	NBatch  `protobuf:"bytes,4,opt,name=batch,embedded=batch" json:"batch"`
}

func (m *BatchChunk) Reset()         { *m = BatchChunk{} }
func (m *BatchChunk) String() string { return proto1.CompactTextString(m) }
func (*BatchChunk) ProtoMessage()    {}

func (m *BatchChunk) GetReplica() Replica {
	if m != nil {
		return m.Replica
	}
	return Replica{}
}

func (m *BatchChunk) GetRangeID() RangeID {
	if m != nil {
		return m.RangeID
	}
	return 0
}

func (m *KeyHeader) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
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
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthRefactored
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append([]byte{}, data[iNdEx:postIndex]...)
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field EndKey", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthRefactored
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.EndKey = append([]byte{}, data[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			iNdEx -= sizeOfWire
			skippy, err := skipRefactored(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRefactored
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return nil
}
func (m *NBatch) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
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
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CmdID", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthRefactored
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CmdID.Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field User", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
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
			postIndex := iNdEx + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.User = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field UserPriority", wireType)
			}
			var v int32
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.UserPriority = &v
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Txn", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthRefactored
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Txn == nil {
				m.Txn = &Transaction{}
			}
			if err := m.Txn.Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Requests", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthRefactored
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Requests = append(m.Requests, RequestUnion{})
			if err := m.Requests[len(m.Requests)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			iNdEx -= sizeOfWire
			skippy, err := skipRefactored(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRefactored
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return nil
}
func (m *BatchChunk) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
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
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Replica", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthRefactored
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.Replica.Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field RangeID", wireType)
			}
			m.RangeID = 0
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.RangeID |= (RangeID(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field NBatch", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthRefactored
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.NBatch.Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			iNdEx -= sizeOfWire
			skippy, err := skipRefactored(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRefactored
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return nil
}
func skipRefactored(data []byte) (n int, err error) {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
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
			for {
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
				return 0, ErrInvalidLengthRefactored
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
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
				next, err := skipRefactored(data[start:])
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
	ErrInvalidLengthRefactored = fmt.Errorf("proto: negative length found during unmarshaling")
)

func (m *KeyHeader) Size() (n int) {
	var l int
	_ = l
	if m.Key != nil {
		l = len(m.Key)
		n += 1 + l + sovRefactored(uint64(l))
	}
	if m.EndKey != nil {
		l = len(m.EndKey)
		n += 1 + l + sovRefactored(uint64(l))
	}
	return n
}

func (m *NBatch) Size() (n int) {
	var l int
	_ = l
	l = m.CmdID.Size()
	n += 1 + l + sovRefactored(uint64(l))
	l = len(m.User)
	n += 1 + l + sovRefactored(uint64(l))
	if m.UserPriority != nil {
		n += 1 + sovRefactored(uint64(*m.UserPriority))
	}
	if m.Txn != nil {
		l = m.Txn.Size()
		n += 1 + l + sovRefactored(uint64(l))
	}
	if len(m.Requests) > 0 {
		for _, e := range m.Requests {
			l = e.Size()
			n += 1 + l + sovRefactored(uint64(l))
		}
	}
	return n
}

func (m *BatchChunk) Size() (n int) {
	var l int
	_ = l
	l = m.Replica.Size()
	n += 1 + l + sovRefactored(uint64(l))
	n += 1 + sovRefactored(uint64(m.RangeID))
	l = m.NBatch.Size()
	n += 1 + l + sovRefactored(uint64(l))
	return n
}

func sovRefactored(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozRefactored(x uint64) (n int) {
	return sovRefactored(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *KeyHeader) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *KeyHeader) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Key != nil {
		data[i] = 0xa
		i++
		i = encodeVarintRefactored(data, i, uint64(len(m.Key)))
		i += copy(data[i:], m.Key)
	}
	if m.EndKey != nil {
		data[i] = 0x12
		i++
		i = encodeVarintRefactored(data, i, uint64(len(m.EndKey)))
		i += copy(data[i:], m.EndKey)
	}
	return i, nil
}

func (m *NBatch) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *NBatch) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0xa
	i++
	i = encodeVarintRefactored(data, i, uint64(m.CmdID.Size()))
	n1, err := m.CmdID.MarshalTo(data[i:])
	if err != nil {
		return 0, err
	}
	i += n1
	data[i] = 0x12
	i++
	i = encodeVarintRefactored(data, i, uint64(len(m.User)))
	i += copy(data[i:], m.User)
	if m.UserPriority != nil {
		data[i] = 0x18
		i++
		i = encodeVarintRefactored(data, i, uint64(*m.UserPriority))
	}
	if m.Txn != nil {
		data[i] = 0x22
		i++
		i = encodeVarintRefactored(data, i, uint64(m.Txn.Size()))
		n2, err := m.Txn.MarshalTo(data[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	if len(m.Requests) > 0 {
		for _, msg := range m.Requests {
			data[i] = 0x2a
			i++
			i = encodeVarintRefactored(data, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *BatchChunk) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *BatchChunk) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0xa
	i++
	i = encodeVarintRefactored(data, i, uint64(m.Replica.Size()))
	n3, err := m.Replica.MarshalTo(data[i:])
	if err != nil {
		return 0, err
	}
	i += n3
	data[i] = 0x10
	i++
	i = encodeVarintRefactored(data, i, uint64(m.RangeID))
	data[i] = 0x22
	i++
	i = encodeVarintRefactored(data, i, uint64(m.NBatch.Size()))
	n4, err := m.NBatch.MarshalTo(data[i:])
	if err != nil {
		return 0, err
	}
	i += n4
	return i, nil
}

func encodeFixed64Refactored(data []byte, offset int, v uint64) int {
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
func encodeFixed32Refactored(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintRefactored(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
