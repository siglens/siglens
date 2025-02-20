package log

import (
	reflect "reflect"
	sync "sync"

	_ "github.com/gogo/protobuf/gogoproto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type PushRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Streams []*StreamAdapter `protobuf:"bytes,1,rep,name=streams,proto3" json:"streams,omitempty"`
}

func (x *PushRequest) Reset() {
	*x = PushRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_loki_push_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushRequest) ProtoMessage() {}

func (x *PushRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_loki_push_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushRequest.ProtoReflect.Descriptor instead.
func (*PushRequest) Descriptor() ([]byte, []int) {
	return file_pkg_loki_push_proto_rawDescGZIP(), []int{0}
}

func (x *PushRequest) GetStreams() []*StreamAdapter {
	if x != nil {
		return x.Streams
	}
	return nil
}

type PushResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *PushResponse) Reset() {
	*x = PushResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_loki_push_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PushResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PushResponse) ProtoMessage() {}

func (x *PushResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_loki_push_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PushResponse.ProtoReflect.Descriptor instead.
func (*PushResponse) Descriptor() ([]byte, []int) {
	return file_pkg_loki_push_proto_rawDescGZIP(), []int{1}
}

type StreamAdapter struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Labels  string          `protobuf:"bytes,1,opt,name=labels,proto3" json:"labels,omitempty"`
	Entries []*EntryAdapter `protobuf:"bytes,2,rep,name=entries,proto3" json:"entries,omitempty"`
	// hash contains the original hash of the stream.
	Hash uint64 `protobuf:"varint,3,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (x *StreamAdapter) Reset() {
	*x = StreamAdapter{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_loki_push_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StreamAdapter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamAdapter) ProtoMessage() {}

func (x *StreamAdapter) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_loki_push_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StreamAdapter.ProtoReflect.Descriptor instead.
func (*StreamAdapter) Descriptor() ([]byte, []int) {
	return file_pkg_loki_push_proto_rawDescGZIP(), []int{2}
}

func (x *StreamAdapter) GetLabels() string {
	if x != nil {
		return x.Labels
	}
	return ""
}

func (x *StreamAdapter) GetEntries() []*EntryAdapter {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *StreamAdapter) GetHash() uint64 {
	if x != nil {
		return x.Hash
	}
	return 0
}

type EntryAdapter struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Line      string                 `protobuf:"bytes,2,opt,name=line,proto3" json:"line,omitempty"`
}

func (x *EntryAdapter) Reset() {
	*x = EntryAdapter{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_loki_push_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EntryAdapter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EntryAdapter) ProtoMessage() {}

func (x *EntryAdapter) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_loki_push_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EntryAdapter.ProtoReflect.Descriptor instead.
func (*EntryAdapter) Descriptor() ([]byte, []int) {
	return file_pkg_loki_push_proto_rawDescGZIP(), []int{3}
}

func (x *EntryAdapter) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *EntryAdapter) GetLine() string {
	if x != nil {
		return x.Line
	}
	return ""
}

var File_pkg_loki_push_proto protoreflect.FileDescriptor

var file_pkg_loki_push_proto_rawDesc = []byte{
	0x0a, 0x13, 0x70, 0x6b, 0x67, 0x2f, 0x6c, 0x6f, 0x6b, 0x69, 0x2f, 0x70, 0x75, 0x73, 0x68, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x6c, 0x6f, 0x6b, 0x69, 0x1a, 0x1b, 0x70, 0x6b, 0x67,
	0x2f, 0x6c, 0x6f, 0x6b, 0x69, 0x2f, 0x66, 0x6f, 0x72, 0x65, 0x69, 0x67, 0x6e, 0x2f, 0x67, 0x6f,
	0x67, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x20, 0x70, 0x6b, 0x67, 0x2f, 0x6c, 0x6f,
	0x6b, 0x69, 0x2f, 0x66, 0x6f, 0x72, 0x65, 0x69, 0x67, 0x6e, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x53, 0x0a, 0x0b, 0x50, 0x75,
	0x73, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x44, 0x0a, 0x07, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x6c, 0x6f, 0x6b,
	0x69, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x41, 0x64, 0x61, 0x70, 0x74, 0x65, 0x72, 0x42,
	0x15, 0xda, 0xde, 0x1f, 0x06, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xea, 0xde, 0x1f, 0x07, 0x73,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x73, 0x52, 0x07, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73, 0x22,
	0x0e, 0x0a, 0x0c, 0x50, 0x75, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x8d, 0x01, 0x0a, 0x0d, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x41, 0x64, 0x61, 0x70, 0x74, 0x65,
	0x72, 0x12, 0x22, 0x0a, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x42, 0x0a, 0xea, 0xde, 0x1f, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x52, 0x06, 0x6c,
	0x61, 0x62, 0x65, 0x6c, 0x73, 0x12, 0x3d, 0x0a, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73,
	0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x6c, 0x6f, 0x6b, 0x69, 0x2e, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x41, 0x64, 0x61, 0x70, 0x74, 0x65, 0x72, 0x42, 0x0f, 0xc8, 0xde, 0x1f, 0x00,
	0xea, 0xde, 0x1f, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x07, 0x65, 0x6e, 0x74,
	0x72, 0x69, 0x65, 0x73, 0x12, 0x19, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x04, 0x42, 0x05, 0xea, 0xde, 0x1f, 0x01, 0x2d, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x22,
	0x6b, 0x0a, 0x0c, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x41, 0x64, 0x61, 0x70, 0x74, 0x65, 0x72, 0x12,
	0x3d, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x6c, 0x6f, 0x6b, 0x69, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x42, 0x0e, 0xc8, 0xde, 0x1f, 0x00, 0xea, 0xde, 0x1f, 0x02, 0x74, 0x73, 0x90,
	0xdf, 0x1f, 0x01, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1c,
	0x0a, 0x04, 0x6c, 0x69, 0x6e, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x42, 0x08, 0xea, 0xde,
	0x1f, 0x04, 0x6c, 0x69, 0x6e, 0x65, 0x52, 0x04, 0x6c, 0x69, 0x6e, 0x65, 0x32, 0x39, 0x0a, 0x06,
	0x50, 0x75, 0x73, 0x68, 0x65, 0x72, 0x12, 0x2f, 0x0a, 0x04, 0x50, 0x75, 0x73, 0x68, 0x12, 0x11,
	0x2e, 0x6c, 0x6f, 0x6b, 0x69, 0x2e, 0x50, 0x75, 0x73, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x12, 0x2e, 0x6c, 0x6f, 0x6b, 0x69, 0x2e, 0x50, 0x75, 0x73, 0x68, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x06, 0x5a, 0x04, 0x2f, 0x6c, 0x6f, 0x67, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_loki_push_proto_rawDescOnce sync.Once
	file_pkg_loki_push_proto_rawDescData = file_pkg_loki_push_proto_rawDesc
)

func file_pkg_loki_push_proto_rawDescGZIP() []byte {
	file_pkg_loki_push_proto_rawDescOnce.Do(func() {
		file_pkg_loki_push_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_loki_push_proto_rawDescData)
	})
	return file_pkg_loki_push_proto_rawDescData
}

var file_pkg_loki_push_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_loki_push_proto_goTypes = []interface{}{
	(*PushRequest)(nil),           // 0: loki.PushRequest
	(*PushResponse)(nil),          // 1: loki.PushResponse
	(*StreamAdapter)(nil),         // 2: loki.StreamAdapter
	(*EntryAdapter)(nil),          // 3: loki.EntryAdapter
	(*timestamppb.Timestamp)(nil), // 4: loki.Timestamp
}
var file_pkg_loki_push_proto_depIdxs = []int32{
	2, // 0: loki.PushRequest.streams:type_name -> loki.StreamAdapter
	3, // 1: loki.StreamAdapter.entries:type_name -> loki.EntryAdapter
	4, // 2: loki.EntryAdapter.timestamp:type_name -> loki.Timestamp
	0, // 3: loki.Pusher.Push:input_type -> loki.PushRequest
	1, // 4: loki.Pusher.Push:output_type -> loki.PushResponse
	4, // [4:5] is the sub-list for method output_type
	3, // [3:4] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_pkg_loki_push_proto_init() }
func file_pkg_loki_push_proto_init() {
	if File_pkg_loki_push_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_loki_push_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_loki_push_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PushResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_loki_push_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StreamAdapter); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_loki_push_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EntryAdapter); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_loki_push_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_loki_push_proto_goTypes,
		DependencyIndexes: file_pkg_loki_push_proto_depIdxs,
		MessageInfos:      file_pkg_loki_push_proto_msgTypes,
	}.Build()
	File_pkg_loki_push_proto = out.File
	file_pkg_loki_push_proto_rawDesc = nil
	file_pkg_loki_push_proto_goTypes = nil
	file_pkg_loki_push_proto_depIdxs = nil
}
