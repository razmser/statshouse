// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Перенесено из ClickHouse dbms/src/AggregateFunctions/UniquesHashSet.h

type (
	ChUnique struct {
		buf         []uint32
		itemsCount  int32
		sizeDegree  uint32
		skipDegree  uint32
		hasZeroItem bool
	}
)

const (
	// The maximum degree of buffer size before the values are discarded
	uniquesHashMaxSizeDegree = 17

	// The maximum number of elements before the values are discarded
	uniquesHashMaxSize = 1 << (uniquesHashMaxSizeDegree - 1)

	// The number of least significant bits used for thinning. The remaining high-order bits are used to determine the position in the hash table.
	// (high-order bits are taken because the younger bits will be constant after dropping some of the values)
	uniquesHashBitsForSkip = 32 - uniquesHashMaxSizeDegree

	// Initial buffer size degree
	uniquesHashSetInitialSizeDegree = 4

	// The highest skip degree a set can reach. shrinkIfNeed raises the degree
	// to d only while the set holds over uniquesHashMaxSize items, and at
	// degree e every stored hash is a distinct nonzero uint32 with the low e
	// bits zero — at most 2^(32-e)-1 of those exist, plus the zero item, so at
	// degree 16 the set tops out at exactly uniquesHashMaxSize items and the
	// raise loop can never push past it.
	uniquesHashMaxSkipDegree = 16

	// Golang-specific optimization. If buffer is larger, we free it on Reset, if smaller - reuse it
	maxReuseBufferCap = 1024 / 4
)

// C++ implementation uses constructor, here we call Reset method on (ch.buf == nil) in all public methods

func (ch *ChUnique) Reset() {
	ch.sizeDegree = uniquesHashSetInitialSizeDegree
	bufLen := 1 << ch.sizeDegree

	if cap(ch.buf) < bufLen || cap(ch.buf) > maxReuseBufferCap {
		ch.buf = make([]uint32, bufLen)
	} else {
		ch.buf = ch.buf[:bufLen]
		for i := range ch.buf {
			ch.buf[i] = 0
		}
	}

	ch.itemsCount = 0
	ch.skipDegree = 0
	ch.hasZeroItem = false
}

// # of non-zero items in array + int(hasZeroItem)
func (ch *ChUnique) ItemsCount() int {
	return int(ch.itemsCount)
}

func (ch *ChUnique) HeapAllocBytes() int {
	return cap(ch.buf) * 4
}

func (ch *ChUnique) bufSize() int {
	return 1 << ch.sizeDegree
}

func (ch *ChUnique) maxFill() int32 {
	return 1 << (ch.sizeDegree - 1)
}

func (ch *ChUnique) mask() int {
	return (1 << ch.sizeDegree) - 1
}

func (ch *ChUnique) place(x uint32) int {
	return int(x>>uniquesHashBitsForSkip) & ch.mask()
}

// The value is divided by 2 ^ skipDegree
func (ch *ChUnique) good(x uint32) bool {
	return x == ((x >> ch.skipDegree) << ch.skipDegree)
}

// Перенос из ClickHouse dbms/src/Common/HashTable/Hash.h
func (ch *ChUnique) uintHash32(key uint64) uint32 {
	key = (^key) + (key << 18)
	key ^= (key >> 31) | (key << 33)
	key *= 21
	key ^= (key >> 11) | (key << 53)
	key += key << 6
	key ^= (key >> 22) | (key << 42)

	return uint32(key)
}

// Delete all values whose hashes do not divide by 2 ^ skipDegree
func (ch *ChUnique) rehash() {
	for i := 0; i < ch.bufSize(); i++ {
		if ch.buf[i] == 0 {
			continue
		}
		if !ch.good(ch.buf[i]) {
			ch.buf[i] = 0
			ch.itemsCount--
		} else if i != ch.place(ch.buf[i]) {
			// After removing the elements, there may have been room for items,
			//   which were placed further than necessary, due to a collision.
			// You need to move them.
			x := ch.buf[i]
			ch.buf[i] = 0
			ch.reinsertImpl(x)
		}
	}

	// We must process first collision resolution chain once again.
	// Look at the comment in "resize" function.
	for i := 0; i < ch.bufSize() && ch.buf[i] != 0; i++ {
		if i != ch.place(ch.buf[i]) {
			x := ch.buf[i]
			ch.buf[i] = 0
			ch.reinsertImpl(x)
		}
	}
}

// Increase the size of the buffer 2 times or up to newSizeDegree, if it is non-zero
func (ch *ChUnique) resize(newSizeDegree uint32) {
	oldSize := ch.bufSize()
	ch.sizeDegree = newSizeDegree
	bufLen := ch.bufSize()

	// Expand the space
	if cap(ch.buf) < bufLen {
		oldBuf := ch.buf
		ch.buf = make([]uint32, bufLen)
		copy(ch.buf, oldBuf)
	} else {
		ch.buf = ch.buf[:bufLen]
		for i := oldSize; i < bufLen; i++ { // NOP if oldSize >= bufLen
			ch.buf[i] = 0
		}
	}

	// Now some items may need to be moved to a new location.
	// The element can stay in place, or move to a new location "on the right",
	//   or move to the left of the collision resolution chain, because the elements to the left of it have been moved to the new "right" location.
	// There is also a special case
	//    if the element was to be at the end of the old buffer,                        [        x]
	//    but is at the beginning because of the collision resolution chain,            [o       x]
	//    then after resizing, it will first be out of place again,                     [        xo        ]
	//    and in order to transfer it to where you need it,
	//    will have to be after transferring all elements from the old half             [         o   x    ]
	//    process another tail from the collision resolution chain immediately after it [        o    x    ]
	// This is why || buf[i] below.
	for i := 0; i < oldSize || ch.buf[i] != 0; i++ {
		x := ch.buf[i]
		if x == 0 {
			continue
		}

		placeValue := ch.place(x)

		// The element is in its place
		if placeValue == i {
			continue
		}

		for (ch.buf[placeValue] != 0) && (ch.buf[placeValue] != x) {
			placeValue = (placeValue + 1) & ch.mask()
		}

		// The element remained in its place
		if ch.buf[placeValue] == x {
			continue
		}

		ch.buf[placeValue] = x
		ch.buf[i] = 0
	}
}

// Insert a value
func (ch *ChUnique) insertImpl(x uint32) {
	if x == 0 {
		if !ch.hasZeroItem {
			ch.itemsCount++
			ch.hasZeroItem = true
		}
		return
	}

	placeValue := ch.place(x)
	for {
		val := ch.buf[placeValue]
		if val == x {
			return
		}
		if val == 0 {
			ch.buf[placeValue] = x
			ch.itemsCount++
			return
		}
		placeValue = (placeValue + 1) & ch.mask()
	}
}

// Insert a value into the new buffer that was in the old buffer.
// Used when increasing the size of the buffer, as well as when reading from a file.
func (ch *ChUnique) reinsertImpl(x uint32) {
	placeValue := ch.place(x)
	for ch.buf[placeValue] != 0 {
		placeValue = (placeValue + 1) & ch.mask()
	}
	ch.buf[placeValue] = x
}

// If the hash table is full enough, then do resize.
// If there are too many items, then throw half the pieces until they are small enough.
func (ch *ChUnique) shrinkIfNeed() {
	if ch.itemsCount <= ch.maxFill() {
		return
	}

	if ch.itemsCount > uniquesHashMaxSize {
		for ch.itemsCount > uniquesHashMaxSize {
			ch.skipDegree++
			ch.rehash()
		}
	} else {
		ch.resize(ch.sizeDegree + 1)
	}
}

func (ch *ChUnique) Insert(val uint64) {
	if ch.buf == nil {
		ch.Reset()
	}
	hashValue := ch.uintHash32(val)
	ch.insertHash(hashValue)
}

func (ch *ChUnique) insertHash(hashValue uint32) {
	if !ch.good(hashValue) {
		return
	}
	ch.insertImpl(hashValue)
	ch.shrinkIfNeed()
}

func (ch *ChUnique) Size(asIs bool) uint64 {
	if ch.skipDegree == 0 {
		return uint64(ch.itemsCount)
	}

	res := int64(ch.itemsCount) * (1 << ch.skipDegree)

	if asIs {
		return uint64(res) // так и оставить?
	}

	// Pseudo-random remainder - in order to be not visible,
	//   that the number is divided by the power of two.
	res += int64(ch.uintHash32(uint64(ch.itemsCount)) & ((1 << ch.skipDegree) - 1))

	// Correction of a systematic error due to collisions during hashing in UInt32.
	// `fixed_res(res)` formula
	// - with how many different elements of fixed_res,
	//   when randomly scattered across 2^32 buckets,
	//   filled buckets with average of res is obtained.
	const p32 = 1 << 32
	fixedRes := math.Round(p32 * (math.Log(p32) - math.Log(p32-float64(res))))
	return uint64(fixedRes)
}

func (ch *ChUnique) Merge(rhs ChUnique) {
	if rhs.buf == nil {
		return // Merge with empty is NOP
	}
	if ch.buf == nil {
		ch.Reset()
	}
	if rhs.skipDegree > ch.skipDegree {
		ch.skipDegree = rhs.skipDegree
		ch.rehash()
	}
	if !ch.hasZeroItem && rhs.hasZeroItem {
		ch.hasZeroItem = true
		ch.itemsCount++
		ch.shrinkIfNeed()
	}
	for i := 0; i < rhs.bufSize(); i++ {
		// Screen with the target's filter, not the source's: ch.skipDegree can be
		// higher than rhs.skipDegree (already, or raised by shrinkIfNeed mid-loop),
		// and Size() extrapolates itemsCount by 1 << ch.skipDegree, so admitting an
		// item rhs.good but !ch.good inflates the estimate.
		if rhs.buf[i] != 0 && ch.good(rhs.buf[i]) {
			ch.insertImpl(rhs.buf[i])
			ch.shrinkIfNeed()
		}
	}
}

// Saves to clickhouse format for inserting using RowBinary or other binary format
func (ch *ChUnique) MarshallAppend(buf []byte) []byte {
	if ch.buf == nil {
		return append(buf, 0, 0) // SkipDegree, ItemCount
	}

	var tmp [binary.MaxVarintLen64]byte

	buf = append(buf, uint8(ch.skipDegree))

	n := binary.PutUvarint(tmp[:], uint64(ch.itemsCount))
	buf = append(buf, tmp[:n]...)

	if ch.hasZeroItem {
		binary.LittleEndian.PutUint32(tmp[:], 0)
		buf = append(buf, tmp[:4]...)
	}

	for i := 0; i < ch.bufSize(); i++ {
		if ch.buf[i] != 0 {
			binary.LittleEndian.PutUint32(tmp[:], ch.buf[i])
			buf = append(buf, tmp[:4]...)
		}
	}

	return buf
}

func (ch *ChUnique) MarshallAppendEstimatedSize() int {
	return 1 + 3 + 4*int(ch.itemsCount) // skipDegree + estimate of varint (which is not large here)
}

func (ch *ChUnique) UmMarshall(r *bytes.Buffer) error {
	skipDegree, itemsCount, err := readUniquesHeader(r)
	if err != nil {
		return err
	}
	ch.hasZeroItem = false
	ch.skipDegree = uint32(skipDegree)
	ch.itemsCount = int32(itemsCount)
	ch.sizeDegree = uniquesHashSetInitialSizeDegree
	if itemsCount > 1 {
		ch.sizeDegree = uint32(math.Log2(float64(itemsCount)) + 2)
		if ch.sizeDegree < uniquesHashSetInitialSizeDegree {
			ch.sizeDegree = uniquesHashSetInitialSizeDegree
		}
	}
	bufLen := 1 << ch.sizeDegree

	if cap(ch.buf) < bufLen {
		ch.buf = make([]uint32, bufLen)
	} else {
		ch.buf = ch.buf[:bufLen]
		for i := range ch.buf {
			ch.buf[i] = 0
		}
	}

	var tmp [4]byte
	for i := 0; i < int(itemsCount); i++ {
		if _, err = io.ReadFull(r, tmp[:]); err != nil {
			return err
		}
		x := binary.LittleEndian.Uint32(tmp[:])
		if x == 0 {
			ch.hasZeroItem = true
			continue
		}
		ch.reinsertImpl(x)
	}
	return nil
}

// checkUniquesPayload verifies the receiver buffer still holds the full item
// payload before a decode starts mutating state. bytes.Buffer.Read returns
// (n < 4, nil) for a 1-3 byte tail, so without this check a blob truncated
// mid-payload would either mis-decode its last item from stale bytes with no
// error at all, or fail half-way through — after rehash()/resize() already
// thinned the accumulator whose decode error the caller ignores.
func checkUniquesPayload(r *bytes.Buffer, itemsCount uint64) error {
	if r.Len() < 4*int(itemsCount) {
		return fmt.Errorf("ChUnique payload has %d bytes for %d items, want %d", r.Len(), itemsCount, 4*itemsCount)
	}
	return nil
}

// readUniquesHeader reads one state blob's header and validates the whole blob
// — header and item payload — before the caller's decode starts mutating the
// receiver, so a rejected blob leaves the accumulator exactly as it was. The
// buffer is left positioned at the first item byte.
func readUniquesHeader(r *bytes.Buffer) (skipDegree byte, itemsCount uint64, err error) {
	skipDegree, err = r.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	if err := checkUniquesSkipDegree(skipDegree); err != nil {
		return 0, 0, err
	}
	itemsCount, err = binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, err
	}
	if itemsCount > uniquesHashMaxSize {
		return 0, 0, fmt.Errorf("ChUnique has too many (%d) items", itemsCount)
	}
	// A state at the maximum skip degree is never empty: shrinkIfNeed raises
	// the degree a step only while the set holds over uniquesHashMaxSize
	// items, and at degree 15 only the 2^16 distinct odd multiples of 2^15
	// fail degree 16, so a set crossing to 16 keeps at least one item (the
	// zero item itself survives every raise). A {max, 0} blob is corrupt or
	// hostile, not a state this codec produces: merging it would raise a
	// lower-degree target to the maximum and rehash away all but one in 2^16
	// of its hashes, wiping the accumulator with a success return.
	if skipDegree == uniquesHashMaxSkipDegree && itemsCount == 0 {
		return 0, 0, fmt.Errorf("ChUnique cannot be empty at skip degree %d", skipDegree)
	}
	if err := checkUniquesPayload(r, itemsCount); err != nil {
		return 0, 0, err
	}
	if err := checkUniquesItems(r.Bytes(), uint32(skipDegree), itemsCount); err != nil {
		return 0, 0, err
	}
	return skipDegree, itemsCount, nil
}

// checkUniquesItems validates the item payload of a state blob against its own
// header: every nonzero hash must end in skipDegree zero bits — the codec and
// ClickHouse's UniquesHashSet only ever store such hashes (insertHash screens
// every insert and every degree raise rehashes the table) — and the payload's
// distinct hashes, with the zero item counted once, must add up to the declared
// count. A blob failing either check is corrupt or hostile, not a state this
// codec produces: a plausible degree with unaligned hashes ({16, 1,
// 0x12345678}) passes every length check, then raises the target's skipDegree
// and rehashes — keeping only 1 in 2^16 of its items — before good() silently
// drops every blob item, erasing the accumulator while returning success; a
// duplicated hash ({16, 2, A, A}) does the same with alignment intact.
// payload is the blob's item bytes (r.Bytes() of a checked payload).
func checkUniquesItems(payload []byte, skipDegree uint32, itemsCount uint64) error {
	// Small sets — the common ingestion case — deduplicate over a stack array,
	// spilling into a map only past it, so a typical merge allocates nothing.
	var small [8]uint32
	var seen map[uint32]struct{}
	distinct, zero := 0, false
	for i := uint64(0); i < itemsCount; i++ {
		x := binary.LittleEndian.Uint32(payload[i*4 : i*4+4])
		if x == 0 {
			zero = true
			continue
		}
		if skipDegree > 0 && x&(1<<skipDegree-1) != 0 {
			return fmt.Errorf("ChUnique item %#x does not end in %d zero bits", x, skipDegree)
		}
		dup := false
		for _, v := range small {
			if v == x {
				dup = true
				break
			}
		}
		if !dup && seen != nil {
			_, dup = seen[x]
		}
		if dup {
			return fmt.Errorf("ChUnique item %#x appears more than once", x)
		}
		if len(seen) == 0 && distinct < len(small) {
			small[distinct] = x
		} else if seen == nil {
			seen = make(map[uint32]struct{}, int(itemsCount)-len(small))
			for _, v := range small {
				seen[v] = struct{}{}
			}
			seen[x] = struct{}{}
		} else {
			seen[x] = struct{}{}
		}
		distinct++
	}
	have := distinct
	if zero {
		have++
	}
	if uint64(have) != itemsCount {
		return fmt.Errorf("ChUnique declares %d items but its payload holds %d distinct", itemsCount, have)
	}
	return nil
}

// checkUniquesSkipDegree rejects a skip degree no accumulator can reach (see
// uniquesHashMaxSkipDegree) — corrupt or hostile bytes, not a state this codec
// or ClickHouse's UniquesHashSet produces. Adopting one anyway raises the
// target's degree and rehashes, and good() then rejects every hash that does
// not end in that many zero bits — every nonzero hash from degree 32 up — so
// the merge thins or erases the accumulator yet returns success: a payload of
// {0xff, 0x00} (degree 255, zero items) passes every length check on its way
// to wiping the table clean.
func checkUniquesSkipDegree(skipDegree byte) error {
	if uint32(skipDegree) > uniquesHashMaxSkipDegree {
		return fmt.Errorf("ChUnique has impossible skip degree %d, want at most %d", skipDegree, uniquesHashMaxSkipDegree)
	}
	return nil
}

func (ch *ChUnique) MergeRead(r *bytes.Buffer) error {
	if ch.buf == nil {
		return ch.UmMarshall(r)
	}

	// Read and validate the whole blob before touching the receiver: the
	// ingestion caller ignores this error, so a malformed blob (truncated
	// after the skip-degree byte, an impossible skip degree, a bogus item
	// count, an item payload cut short, or items no encoder could have
	// written — unaligned at the declared degree, duplicated, or fewer
	// distinct than declared) must leave the accumulator exactly as it was —
	// raising skipDegree and rehashing first would irreversibly thin the
	// existing hashes on the way to that error or, for items good() then
	// silently drops, past it.
	skipDegree, itemsCount, err := readUniquesHeader(r)
	if err != nil {
		return err
	}
	if uint32(skipDegree) > ch.skipDegree {
		// Raise the degree before rehashing, exactly as Merge does: rehash()
		// filters by ch.skipDegree and Size() extrapolates itemsCount by
		// 1 << skipDegree, so rehashing without raising it keeps the table's
		// own filter at the old, lower degree while the loop below admits the
		// blob's items — every one of them then counts for 2^old degrees
		// instead of the 2^skipDegree it represents, and the merged set
		// undercounts.
		ch.skipDegree = uint32(skipDegree)
		ch.rehash()
	}
	if (1 << ch.sizeDegree) < itemsCount {
		newSD := uint32(math.Log2(float64(itemsCount)) + 2)
		if newSD < uniquesHashSetInitialSizeDegree {
			newSD = uniquesHashSetInitialSizeDegree
		}
		ch.resize(newSD)
	}
	var tmp [4]byte
	for i := 0; i < int(itemsCount); i++ {
		if _, err = io.ReadFull(r, tmp[:]); err != nil {
			return err
		}
		x := binary.LittleEndian.Uint32(tmp[:])
		ch.insertHash(x)
	}
	return nil
}

func (ch *ChUnique) Marshall(dst io.Writer) error {
	if ch.buf == nil {
		ch.Reset()
	}

	var tmp [binary.MaxVarintLen64]byte

	tmp[0] = uint8(ch.skipDegree)
	if _, err := dst.Write(tmp[:1]); err != nil {
		return err
	}

	if n := binary.PutUvarint(tmp[:], uint64(ch.itemsCount)); true {
		if _, err := dst.Write(tmp[:n]); err != nil {
			return err
		}
	}

	if ch.hasZeroItem {
		binary.LittleEndian.PutUint32(tmp[:], 0)
		if _, err := dst.Write(tmp[:4]); err != nil {
			return err
		}
	}

	for i := 0; i < ch.bufSize(); i++ {
		if ch.buf[i] != 0 {
			binary.LittleEndian.PutUint32(tmp[:], ch.buf[i])
			if _, err := dst.Write(tmp[:4]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (u *ChUnique) ReadFrom(r io.ByteReader) error {
	u.hasZeroItem = false
	sd, err := r.ReadByte()
	if err != nil {
		return err
	}
	u.skipDegree = uint32(sd)
	ic, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if ic > uniquesHashMaxSize {
		return fmt.Errorf("ChUnique has too many (%d) items", ic)
	}
	u.itemsCount = int32(ic)
	u.sizeDegree = uniquesHashSetInitialSizeDegree
	if ic > 1 {
		u.sizeDegree = uint32(math.Log2(float64(ic)) + 2)
		if u.sizeDegree < uniquesHashSetInitialSizeDegree {
			u.sizeDegree = uniquesHashSetInitialSizeDegree
		}
	}
	bufLen := 1 << u.sizeDegree

	if cap(u.buf) < bufLen {
		u.buf = make([]uint32, bufLen)
	} else {
		u.buf = u.buf[:bufLen]
		for i := range u.buf {
			u.buf[i] = 0
		}
	}

	for i := 0; i < int(ic); i++ {
		x, err := readUint32LE(r)
		if err != nil {
			return err
		}
		if x == 0 {
			u.hasZeroItem = true
			continue
		}
		u.reinsertImpl(x)
	}
	return nil
}

func readUint32LE(br io.ByteReader) (uint32, error) {
	b0, err := br.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := br.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := br.ReadByte()
	if err != nil {
		return 0, err
	}
	b3, err := br.ReadByte()
	if err != nil {
		return 0, err
	}

	return uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24, nil
}
