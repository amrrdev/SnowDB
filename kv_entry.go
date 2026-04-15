package db0103

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var ErrBadChecksum = errors.New("bad checksum")

type Entry struct {
	key     []byte
	val     []byte
	deleted bool
}

// Serialization -> convert entry into []byte
// Layout: checksum(4) + keyLen(4) + valLen(4) + deleted(1) + key + val
func (ent *Entry) Encode() []byte {
	data := make([]byte, 4+4+4+1+len(ent.key)+len(ent.val))
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(ent.key)))
	copy(data[13:], ent.key)

	if ent.deleted {
		data[12] = 1
	} else {
		binary.LittleEndian.PutUint32(data[8:12], uint32(len(ent.val)))
		copy(data[13+len(ent.key):], ent.val)
	}

	checksum := crc32.ChecksumIEEE(data[4:])
	binary.LittleEndian.PutUint32(data[0:4], checksum)

	return data
}

// [(checksum(4), 3,0,0,0, 1,0,0,0, 0 , 'a', 'b', 'c', 'q']
// [(checksum(4), 3,0,0,0, 0,0,0,0, 1 , 'a', 'b', 'c', ""]
func (ent *Entry) Decode(r io.Reader) error {
	var header [13]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}

	checksum := int(binary.LittleEndian.Uint32(header[:4]))
	klen := int(binary.LittleEndian.Uint32(header[4:8]))
	vlen := int(binary.LittleEndian.Uint32(header[8:12]))
	deleted := header[12]

	data := make([]byte, klen+vlen)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	ent.key = data[:klen]
	if deleted != 0 {
		ent.deleted = true
	} else {
		ent.deleted = false
		ent.val = data[klen:]
	}

	fullData := append(header[4:], data...)
	if checksum != int(crc32.ChecksumIEEE(fullData)) {
		return ErrBadChecksum
	}

	return nil

}
