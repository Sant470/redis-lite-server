/*
	reference - https://rdb.fnordig.de/file_format.html#length-encoding
*/

package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

// OPCODES
const (
	EOF          = 0xFF
	SELECTDB     = 0xFE
	EXPIRETIME   = 0xFD
	EXPIRETIMEMS = 0xFC
	RESIZEDB     = 0xFB
	AUX          = 0xFA
)

type Item struct {
	Val    string
	Expire time.Time
}

type Database struct {
	ID    uint8
	Store map[string]*Item
}

type RDB struct {
	Magic    [5]byte
	Version  [4]byte
	Aux      map[string]string
	Database []Database
}

func NewRDB() *RDB {
	return &RDB{Database: []Database{*NewDatabase(0)}}
}

func NewDatabase(id uint8) *Database {
	return &Database{id, map[string]*Item{}}
}

func parseString(r *bufio.Reader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	switch b >> 6 {
	case 0b00:
		length := int(b & 0b0011_1111)
		value := make([]byte, length)
		_, _ = r.Read(value)
		return string(value), nil
	case 0b01:
		return "", fmt.Errorf("unexpected byte: %b", b)
	case 0b10:
		val := make([]byte, 4)
		_, _ = r.Read(val)
		return string(val), nil
	case 0b11:
		switch int(b & 0b0011_1111) {
		case 0:
			value, _ := r.ReadByte()
			return fmt.Sprint(int8(value)), nil
		case 1:
			val := make([]byte, 2)
			_, _ = r.Read(val)
			return fmt.Sprint(binary.BigEndian.Uint16(val)), nil
		case 2:
			val := make([]byte, 4)
			_, _ = r.Read(val)
			return fmt.Sprint(binary.BigEndian.Uint32(val)), nil
		case 3:
			return "", fmt.Errorf("unexpected byte: %b", b&0b0011_1111)
		}
	default:
		return "", errors.New("unexpected error")
	}
	return "", nil
}

func extractKeyValue(r *bufio.Reader) (string, string, error) {
	key, err := parseString(r)
	if err != nil {
		return "", "", err
	}
	val, err := parseString(r)
	if err != nil {
		return "", "", err
	}
	return key, val, nil
}

func parseRDB(file *os.File) (*RDB, error) {
	r := bufio.NewReader(file)
	rdb := NewRDB()
	_, err := r.Read(rdb.Magic[:])
	if err != nil {
		return nil, err
	}
	_, err = r.Read(rdb.Version[:])
	if err != nil {
		return nil, err
	}
	for {
		opcode, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		var dbIdx uint8
		switch opcode {
		case EOF:
			return rdb, nil
		case AUX:
			_, _ = parseString(r)
			_, _ = parseString(r)
		case RESIZEDB:
			_, _ = r.ReadByte()
			_, _ = r.ReadByte()
		case SELECTDB:
			_, _ = r.ReadByte()
		case EXPIRETIME:
			expiry := make([]byte, 4)
			r.Read(expiry)
			ts := int64(binary.BigEndian.Uint32(expiry))
			// read value type
			_, _ = r.ReadByte()
			key, val, err := extractKeyValue(r)
			if err != nil {
				log.Println("error extracting key: ", err)
			}
			if ts > time.Now().Unix() {
				rdb.Database[dbIdx].Store[key] = &Item{Val: val, Expire: time.Unix(ts, 0)}
			}
		case EXPIRETIMEMS:
			expiry := make([]byte, 8)
			r.Read(expiry)
			tms := int64(binary.LittleEndian.Uint64(expiry))
			// read value type
			_, _ = r.ReadByte()
			key, val, err := extractKeyValue(r)
			if err != nil {
				log.Println("error extracting key: ", err)
			}
			if tms > time.Now().UnixMilli() {
				rdb.Database[dbIdx].Store[key] = &Item{Val: val, Expire: time.Unix(tms/1000, (tms%1000)*int64(time.Millisecond))}
			}
		default:
			key, val, err := extractKeyValue(r)
			if err != nil {
				log.Println("error extracting key: ", err)
			}
			rdb.Database[dbIdx].Store[key] = &Item{Val: val}
		}
	}
}
