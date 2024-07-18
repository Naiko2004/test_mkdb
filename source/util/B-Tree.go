package btree

import (
	"bytes"
	"encoding/binary"
	"log"
)

type BNode struct {
	data []byte // Para poder guardar en el disco luego.
}

const (
	BNODE_NODE = 1 // Nodo interno sin valor
	BNODE_LEAF = 2 // Nodo hoja con valor
)

type BTree struct {
	// puntero a una pagina no nula
	root uint64
	// callbacks para administrar paginas en el disco
	get func(uint64) BNode // dereferencia un puntero
	new func(BNode) uint64 // allocates una nueva pagina
	del func(uint64)       // deallocates una pagina
}

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

// header

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers

func (node BNode) getPointer(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPointer(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list

func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key values

func (node BNode) keyPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}
func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.keyPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4 : klen]
}
func (node BNode) getValue(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.keyPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes

func (node BNode) nbytes() uint16 {
	return node.keyPos(node.nkeys())
}

// returns first kid node whose range intersects key (kid[i] <= key)
// TODO: bisect
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// first key is a copy from parent node
	// so it's always less or equal than key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node

func leafInsert(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// update a key in a leaf node
func leafUpdate(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	log.Println("Updating key... THIS MIGHT NOT WOKR!")
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// copy multiple KVs into the pos
func nodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	assert(srcOld+n <= old.nkeys())
	assert(dstNew+n <= new.nkeys())
	if n == 0 {
		return
	}

	// copy pointers
	for i := uint16(0); i < n; i++ {
		new.setPointer(dstNew+i, old.getPointer(srcOld+i))
	}

	// copy offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: Range is [1,n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// copy keys and values
	begin := old.keyPos(srcOld)
	end := old.keyPos(srcOld + n)

	copy(new.data[new.keyPos(dstNew):], old.data[begin:end])
}

// copy a KV into the pos
func nodeAppendKV(
	new BNode, idx uint16, pointer uint64, key []byte, val []byte,
) {
	// copy pointer
	new.setPointer(idx, pointer)
	// copy KVs
	pos := new.keyPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// offset of next kew
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// Insert a KV into a node, the result might split into 2 nodes.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// result node
	// can be bigger than 1 page and will be split if so.
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert key?
	idx := nodeLookupLE(node, key)
	// act depending on node type
	switch node.btype() {
	case BNODE_LEAF:
		// lead, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert new key after the pos
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("Invalid node type")
	}
	return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	// get and deallocate kid node
	kidPointer := node.getPointer(idx)
	kid := tree.get(kidPointer)
	tree.del(kidPointer)
	// recursively insert into the kid node
	kid = treeInsert(tree, kid, key, val)
	// split result
	nsplit, splited := nodeSplit3(kid)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// split a node into 2 nodes
// second node always fits in page
func nodeSplit2(left BNode, right BNode, old BNode) {

	log.Println("Split node into 2 nodes.. THIS MIGHT NOT WORK!")

	// split the keys
	n := old.nkeys()
	m := n / 2
	left.setHeader(old.btype(), m)
	right.setHeader(old.btype(), n-m)
	nodeAppendRange(left, old, 0, 0, m)
	nodeAppendRange(right, old, 0, m, n-m)

	// split the pointers
	for i := uint16(0); i < m; i++ {
		left.setPointer(i, old.getPointer(i))
	}
	for i := uint16(0); i < n-m; i++ {
		right.setPointer(i, old.getPointer(m+i))
	}

	// Correctly initialize the offsets for both nodes
	left.setOffset(0, 0)
	right.setOffset(0, 0)

	// Correctly split the offsets for the left node
	for i := uint16(1); i <= m; i++ {
		// Calculate the size of the current element by subtracting the previous offset from the current offset in the old node
		size := old.getOffset(i) - old.getOffset(i-1)
		// Accumulate the offset in the left node
		left.setOffset(i, left.getOffset(i-1)+size)
	}

	// Correctly split the offsets for the right node
	for i := uint16(1); i <= n-m; i++ {
		// Calculate the size of the current element by subtracting the previous offset from the current offset in the old node, adjusted for the split
		size := old.getOffset(m+i) - old.getOffset(m+i-1)
		// Accumulate the offset in the right node
		right.setOffset(i, right.getOffset(i-1)+size)
	}

	// Split the keys and values
	begin := old.keyPos(0)
	end := old.keyPos(m)
	copy(left.data[left.keyPos(0):], old.data[begin:end])
	begin = old.keyPos(m)
	end = old.keyPos(n)
	copy(right.data[right.keyPos(0):], old.data[begin:end])

	// split the last pointer
	right.setPointer(n-m, old.getPointer(n))

	// check the size
	assert(left.nbytes() <= BTREE_PAGE_SIZE)
	assert(right.nbytes() <= BTREE_PAGE_SIZE)

}

// split a node if it's too big into 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is too big, split it again
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}

	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-idx-1)
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to delete key?
	idx := nodeLookupLE(node, key)
	// act depending on node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // key not found
		}
		// delete key in leaf
		new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("Invalid node type")
	}
}

// part of treeDelete(): KV deletion from an internal node
func nodeDelete( tree *Btree, node BNoode, idx uint16, key []byte) BNode {
	// recurse into kid
	kidPointer := node.getPointer(idx)
	updated := treeDelete(tree, tree.get(kidPointer), key)
	if len(updated.data) == 0 {
		// key not found in kid
		return BNode{}
	}
	tree.del(kidPointer)

	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	// check merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)

	switch {
		case mergeDir < 0: // merge with left sibling
			merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
			nodeMerge(merged, sibling, updated)
			tree.del(nde.getPointer(idx-1))
			nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
		case mergeDir > 0: // merge with right sibling
			merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
			nodeMerge(merged, updated, sibling)
			tree.del(node.getPointer(idx+1))
			nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
		case mergeDir == 0: // no merge
			assert(updated.nkeys() > 0)
			nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}