package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/zhuyie/golzf"
)

type DatabaseEntry struct {
	DatabaseIndex int
	Key           string
	Value         any
	Expiry        time.Time
}

type Parser struct {
	r *bufio.Reader
}

const (
	// Auxiliary fields. Arbitrary key-value settings
	OP_AUX = 0xFA
	// End of the RDB file
	OP_EOF = 0xFF
	// Expire time in seconds
	OP_EXPIRE_TIME = 0xFD
	// Expire time in milliseconds
	OP_EXPIRE_TIME_MS = 0xFC
	// Hash table sizes for the main keyspace and expires
	OP_RESIZE_DB = 0xFB
	// Database Selector
	OP_SELECT_DB = 0xFE
)

const (
	LENGTH_ENCODING_6_BIT = iota
	LENGTH_ENCODING_14_BIT
	LENGTH_ENCODING_32_BIT
)

const (
	LENGTH_ENCODING_MASK  = 0b11000000 // Mask for the two MSBs in length encoding
	LENGTH_ENCODING_SHIFT = 6          // Number of bits to shift to get encoding type
)

const (
	INTEGER_STRING_8_BIT = iota
	INTEGER_STRING_16_BIT
	INTEGER_STRING_32_BIT
	COMPRESSED_STRING
)

type ValueEncoding int

const (
	STRING_ENCODING ValueEncoding = iota
	LIST_ENCODING
	SET_ENCODING
	SORTED_SET_ENCODING
	HASH_MAP_ENCODING
	ZIP_MAP_ENCODING = iota + 4
	ZIP_LIST_ENCODING
	INT_SET_ENCODING
	SORTED_SET_IN_ZIP_LIST_ENCODING
	HASH_MAP_IN_ZIP_LIST_ENCODING
	LIST_IN_QUICK_LIST_ENCODING
)

var (
	errInvalidSyntax            = errors.New("syntax error")
	errExpectedLengthEncodedInt = errors.New("expected a length-encoded integer")
)

func NewParser() *Parser {
	return &Parser{}
}

func isSectionIndicator(opCode byte) bool {
	return slices.Contains([]byte{OP_AUX, OP_EOF, OP_SELECT_DB}, opCode)
}

func (p *Parser) checkHeader() error {
	magicString := []byte("REDIS")
	magicStringBuf := make([]byte, len(magicString))
	versionNumberBuf := make([]byte, 4)

	if _, err := io.ReadAtLeast(p.r, magicStringBuf, len(magicStringBuf)); err != nil {
		return fmt.Errorf("failed to parse magic string from file: %w", err)
	}

	if !bytes.Equal(magicStringBuf, magicString) {
		return fmt.Errorf("%w: rdb file must begin with magic string \"%s\"", errInvalidSyntax, magicString)
	}

	if _, err := io.ReadAtLeast(p.r, versionNumberBuf, len(versionNumberBuf)); err != nil {
		return fmt.Errorf("failed to parse redis version number from file: %w", err)
	}

	if _, err := strconv.Atoi(string(versionNumberBuf)); err != nil {
		return fmt.Errorf("%w: redis version number is invalid", err)
	}

	return nil
}

func (p *Parser) parseAuxFields() error {
	errMsg := "failed to parse metadata section"

	// we currently don't do anything with the data from this section
	for {
		nextByte, err := p.r.Peek(1)

		if err != nil {
			return fmt.Errorf("%s:%w", errMsg, err)
		}

		if isSectionIndicator(nextByte[0]) {
			break
		}

		if _, err := p.parseString(); err != nil {
			return fmt.Errorf("%s:%w", errMsg, err)
		}

		if _, err := p.parseString(); err != nil {
			return fmt.Errorf("%s:%w", errMsg, err)
		}
	}

	return nil
}

func (p *Parser) parseDatabase() ([]DatabaseEntry, error) {
	errMsg := func(err error) error {
		return fmt.Errorf("failed to parse database: %w", err)
	}

	dbIndex, err := p.parseSize()

	if err != nil {
		return nil, err
	}

	if _, _, err := p.parseDatabaseHashTableSizes(); err != nil {
		return nil, errMsg(err)
	}

	entries := []DatabaseEntry{}

	for {
		buf, err := p.r.Peek(1)

		if err != nil {
			return nil, errMsg(err)
		}

		if isSectionIndicator(buf[0]) {
			return entries, nil
		}

		entry, err := p.parseDatabaseEntry(dbIndex)

		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}
}

func (p *Parser) parseDatabaseEntry(dbIndex int) (DatabaseEntry, error) {
	var entry DatabaseEntry
	expiry := time.Time{}

	b, err := p.r.Peek(1)

	if err != nil {
		return entry, fmt.Errorf("failed to parse database entry: %w", err)
	}

	if slices.Contains([]byte{OP_EXPIRE_TIME, OP_EXPIRE_TIME_MS}, b[0]) {
		timestamp, err := p.parseDatabaseEntryExpiry()

		if err != nil {
			return entry, err
		}

		expiry = timestamp
	}

	valueEncoding, err := p.parseValueEncoding()

	if err != nil {
		return entry, err
	}

	key, err := p.parseString()

	if err != nil {
		return entry, err
	}

	value, err := p.parseValue(valueEncoding)

	if err != nil {
		return entry, err
	}

	entry.DatabaseIndex = dbIndex
	entry.Expiry = expiry
	entry.Key = key
	entry.Value = value

	return entry, nil
}

func (p *Parser) parseDatabaseEntryExpiry() (time.Time, error) {
	errMsg := func(err error) error {
		return fmt.Errorf("failed to parse expire timestamp: %w", err)
	}

	opCode, err := p.r.ReadByte()

	if err != nil {
		return time.Time{}, errMsg(err)
	}

	switch opCode {
	case OP_EXPIRE_TIME:
		bufSize := 4
		buf := make([]byte, bufSize)

		if _, err := io.ReadAtLeast(p.r, buf, bufSize); err != nil {
			return time.Time{}, errMsg(err)
		}

		return time.Unix(int64(binary.LittleEndian.Uint32(buf)), 0), nil

	case OP_EXPIRE_TIME_MS:
		bufSize := 8
		buf := make([]byte, bufSize)

		if _, err := io.ReadAtLeast(p.r, buf, bufSize); err != nil {
			return time.Time{}, errMsg(err)
		}

		return time.UnixMilli(int64(binary.LittleEndian.Uint64(buf))), nil
	default:
		return time.Time{}, fmt.Errorf("unexpected op code: %x", opCode)
	}
}

func (p *Parser) parseDatabaseHashTableSizes() (int, int, error) {
	errMsg := func(err error) error {
		return fmt.Errorf("failed to parse database hash table sizes: %w", err)
	}

	b, err := p.r.Peek(1)

	if err != nil {
		return 0, 0, errMsg(err)
	}

	if b[0] != OP_RESIZE_DB {
		return 0, 0, nil
	}

	// Read and discard the 'OP_RESIZE_DB' op code.
	if _, err := p.r.ReadByte(); err != nil {
		return 0, 0, errMsg(err)
	}

	hashTableSize, err := p.parseSize()

	if err != nil {
		return 0, 0, errMsg(err)
	}

	expireHashTableSize, err := p.parseSize()

	if err != nil {
		return 0, 0, errMsg(err)
	}

	return hashTableSize, expireHashTableSize, nil
}

func (p *Parser) parseHashMap() (map[string]string, error) {
	size, err := p.parseSize()

	if err != nil {
		return nil, fmt.Errorf("failed to parse hash map: %w", err)
	}

	hashMap := make(map[string]string, size)

	for range size {
		key, err := p.parseString()

		if err != nil {
			return nil, fmt.Errorf("failed to parse hash map entry: %w", err)
		}

		value, err := p.parseString()

		if err != nil {
			return nil, fmt.Errorf("failed to parse hash map entry: %w", err)
		}

		hashMap[key] = value
	}

	return hashMap, nil
}

func (p *Parser) parseLength() (int, bool, error) {
	const errMsg = "failed to parse length"

	firstByte, err := p.r.ReadByte()

	if err != nil {
		return 0, false, fmt.Errorf("%s:%w", errMsg, err)
	}

	encodingType := (firstByte & LENGTH_ENCODING_MASK) >> LENGTH_ENCODING_SHIFT
	valueOfLast6Bits := (byte(0b00111111) & firstByte)

	switch encodingType {
	case LENGTH_ENCODING_6_BIT:
		return int(valueOfLast6Bits), false, nil

	case LENGTH_ENCODING_14_BIT:
		{
			nextByte, err := p.r.ReadByte()

			if err != nil {
				return 0, false, fmt.Errorf("%s:%w", errMsg, err)
			}

			buf := []byte{valueOfLast6Bits, nextByte}
			length := binary.BigEndian.Uint16(buf)

			return int(length), false, nil
		}

	case LENGTH_ENCODING_32_BIT:
		{
			size := 4
			buf := make([]byte, size)

			if _, err := io.ReadAtLeast(p.r, buf, size); err != nil {
				return 0, false, fmt.Errorf("%s:%w", errMsg, err)
			}

			return int(binary.BigEndian.Uint32(buf)), false, nil
		}

	default:
		return int(valueOfLast6Bits), true, nil
	}
}

func (p *Parser) parseList() ([]string, error) {
	listSize, err := p.parseSize()

	if err != nil {
		return nil, fmt.Errorf("failed to parse list size: %w", err)
	}

	list := make([]string, listSize)

	for index := range listSize {
		entry, err := p.parseString()

		if err != nil {
			return nil, fmt.Errorf("failed to parse list entry at index %d: %w", index, err)
		}

		list[index] = entry
	}

	return list, nil
}

func (p *Parser) parseCompressedString() (string, error) {
	errMsg := "failed to parse compressed string"
	compressedLength, err := p.parseSize()

	if err != nil {
		return "", fmt.Errorf("failed to parse compressed length:%w", err)
	}

	uncompressedLength, err := p.parseSize()

	if err != nil {
		return "", fmt.Errorf("failed to parse uncompressed length:%w", err)
	}

	inputBuf := make([]byte, compressedLength)
	outputBuf := make([]byte, uncompressedLength)

	if _, err := io.ReadAtLeast(p.r, inputBuf, compressedLength); err != nil {
		return "", fmt.Errorf("%s:%w", errMsg, err)
	}

	n, err := lzf.Decompress(inputBuf, outputBuf)

	if err != nil {
		return "", fmt.Errorf("%s: %v", errMsg, err)
	}

	if n != uncompressedLength {
		return "", fmt.Errorf("%s: decompressed string length %d does not match expected length %d", errMsg, n, uncompressedLength)
	}

	return string(outputBuf), nil
}

func (p *Parser) parseSize() (int, error) {
	size, isSpecial, err := p.parseLength()

	if err != nil {
		return 0, err
	}

	if isSpecial {
		return 0, errExpectedLengthEncodedInt
	}

	return size, nil
}

func (p *Parser) parseString() (string, error) {
	errMsg := "failed to parse string"
	length, isEncoded, err := p.parseLength()

	if err != nil {
		return "", err
	}

	if !isEncoded {
		buf := make([]byte, length)

		if _, err := io.ReadAtLeast(p.r, buf, length); err != nil {
			return "", fmt.Errorf("%s:%w", errMsg, err)
		}

		return string(buf), nil
	}

	switch length {
	case INTEGER_STRING_8_BIT:
		{
			intByte, err := p.r.ReadByte()

			if err != nil {
				return "", fmt.Errorf("%s:%w", errMsg, err)
			}

			return strconv.Itoa(int(intByte)), nil
		}

	case INTEGER_STRING_16_BIT:
		{
			size := 2
			buf := make([]byte, size)

			if _, err := io.ReadAtLeast(p.r, buf, size); err != nil {
				return "", fmt.Errorf("%s:%w", errMsg, err)
			}

			return strconv.Itoa(int(binary.LittleEndian.Uint16(buf))), nil
		}

	case INTEGER_STRING_32_BIT:
		{
			size := 4
			buf := make([]byte, size)

			if _, err := io.ReadAtLeast(p.r, buf, size); err != nil {
				return "", fmt.Errorf("%s:%w", errMsg, err)
			}

			return strconv.Itoa(int(binary.LittleEndian.Uint32(buf))), nil
		}

	case COMPRESSED_STRING:
		{
			return p.parseCompressedString()
		}

	default:
		{
			return "", fmt.Errorf("unsupported string encoding %d", length)
		}
	}
}

func (p *Parser) parseValue(valueEncoding ValueEncoding) (any, error) {
	switch valueEncoding {
	case STRING_ENCODING:
		return p.parseString()

	case LIST_ENCODING, SET_ENCODING:
		return p.parseList()

	case HASH_MAP_ENCODING:
		return p.parseHashMap()

	default:
		return nil, fmt.Errorf("unknown value encoding: %d", valueEncoding)
	}
}

func (p *Parser) parseValueEncoding() (ValueEncoding, error) {
	b, err := p.r.ReadByte()

	if err != nil {
		return 0, fmt.Errorf("failed to read value encoding: %w", err)
	}

	return ValueEncoding(b), nil
}

func (p *Parser) Parse(src string) ([]DatabaseEntry, error) {
	fd, err := os.Open(src)

	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", src, err)
	}

	defer fd.Close()

	p.r = bufio.NewReader(fd)

	if err := p.checkHeader(); err != nil {
		return nil, err
	}

	entries := []DatabaseEntry{}

	for {
		opCode, err := p.r.ReadByte()

		if err != nil {
			return nil, err
		}

		switch opCode {
		case OP_AUX:
			if err := p.parseAuxFields(); err != nil {
				return nil, err
			}

		case OP_SELECT_DB:
			dbEntries, err := p.parseDatabase()

			if err != nil {
				return nil, err
			}

			entries = append(entries, dbEntries...)

		case OP_EOF:
			return entries, nil

		default:
			return nil, fmt.Errorf("%w: unknown op code %x", errInvalidSyntax, opCode)
		}
	}
}
