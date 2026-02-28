package fileparser

type INodeEntry struct {
	FSegID           uint64
	FSegNotNullNUsed uint32
	FSegFree         FLSTBaseNode
	FSegNotNull      FLSTBaseNode
	FSegFull         FLSTBaseNode
	FSegMagicN       uint32
	FSegFragArr      [32]uint32
}
