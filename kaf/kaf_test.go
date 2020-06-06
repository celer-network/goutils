// Copyright 2020 Celer Network

package kaf

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	numReaderFetch  = 0
	numReaderCommit = 0
	numReaderClose  = 0
	numWriterWrite  = 0
	numWriterClose  = 0

	ErrReaderEnd    = errors.New("kafka reader terminated")
	ErrReaderCommit = errors.New("kafka reader cannot commit")
	ErrWriterEnd    = errors.New("kafka writer terminated")
)

func resetCounters() {
	numReaderFetch = 0
	numReaderCommit = 0
	numReaderClose = 0
	numWriterWrite = 0
	numWriterClose = 0
}

func mkMsg(count int) ([]byte, []byte) {
	key := fmt.Sprintf("key-%d", count)
	val := fmt.Sprintf("value-%d", count)
	return []byte(key), []byte(val)
}

func expectMsg(t *testing.T, count int, key, val []byte) {
	expKey, expVal := mkMsg(count)
	if !reflect.DeepEqual(key, expKey) {
		t.Errorf("wrong msg key (%d): %s != %s", count, string(key), string(expKey))
	}
	if !reflect.DeepEqual(val, expVal) {
		t.Errorf("wrong msg val (%d): %s != %s", count, string(val), string(expVal))
	}
}

func expectReaderCounters(t *testing.T, nfetch, ncommit, nclose int) {
	if numReaderFetch != nfetch {
		t.Errorf("wrong reader fetch count: %d != %d", numReaderFetch, nfetch)
	}
	if numReaderCommit != ncommit {
		t.Errorf("wrong reader commit count: %d != %d", numReaderCommit, ncommit)
	}
	if numReaderClose != nclose {
		t.Errorf("wrong reader close count: %d != %d", numReaderClose, nclose)
	}
}

func expectWriterCounters(t *testing.T, nwrite, nclose int) {
	if numWriterWrite != nwrite {
		t.Errorf("wrong writer write count: %d != %d", numWriterWrite, nwrite)
	}
	if numWriterClose != nclose {
		t.Errorf("wrong writer close count: %d != %d", numWriterClose, nclose)
	}
}

type mockReader struct {
	count    int  // message counter
	ttl      int  // number of messages received before disconnect
	nocommit bool // refuse to commit a message
	block    bool // block and never receive a message
}

// For testing purposes, the "topic" of the Kafka config is hijacked and used
// to indicate the type of mock reader to create.
func mkMockReader(config *kafka.ReaderConfig) KafReader {
	mr := &mockReader{}

	switch config.Topic {
	case "block":
		mr.block = true
	case "five-msg":
		mr.ttl = 5
	case "inf-msg":
		mr.ttl = -1
	case "nocommit":
		mr.nocommit = true
		mr.ttl = -1
	default:
		mr.block = true
	}

	return mr
}

func (mr *mockReader) Close() error {
	numReaderClose++
	return nil
}

func (mr *mockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if mr.nocommit {
		return ErrReaderCommit
	}

	numReaderCommit++
	return nil
}

func (mr *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	numReaderFetch++
	msg := kafka.Message{}

	if mr.block {
		<-ctx.Done() // block until context is cancelled
		return msg, ErrReaderEnd
	} else if mr.ttl == 0 {
		return msg, ErrReaderEnd
	} else if mr.ttl > 0 {
		mr.ttl--
	}

	if mr.count > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	msg.Key, msg.Value = mkMsg(mr.count)
	mr.count++
	return msg, nil
}

func newKafRecv(topic string, cb KafRecvFunc) *KafRecv {
	return makeKafRecv("fake-url", "fake-component", topic, cb, mkMockReader)
}

type mockWriter struct {
	count int  // message counter
	ttl   int  // number of messages written before disconnect
	block bool // block and never write a message
}

// For testing purposes, the "topic" of the Kafka config is hijacked and used
// to indicate the type of mock writer to create.
func mkMockWriter(config *kafka.WriterConfig) KafWriter {
	mw := &mockWriter{}

	switch config.Topic {
	case "block":
		mw.block = true
	case "five-msg":
		mw.ttl = 5
	default:
		mw.block = true
	}

	return mw
}

func (mw *mockWriter) Close() error {
	numWriterClose++
	return nil
}

func (mw *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	numWriterWrite++

	if mw.block {
		<-ctx.Done() // block until context is cancelled
		return ErrWriterEnd
	} else if mw.ttl == 0 {
		return ErrWriterEnd
	} else if mw.ttl > 0 {
		mw.ttl--
	}

	if mw.count > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	mw.count++
	return nil
}

func newKafSend(topic string) *KafSend {
	return makeKafSend("fake-url", topic, mkMockWriter)
}

func TestRecv(t *testing.T) {
	resetCounters()

	var kr *KafRecv
	recvCount, maxMsgCount := 0, 6
	done := make(chan bool)
	msgHdl := func(key, val []byte) {
		expectMsg(t, recvCount, key, val)
		recvCount++
		if recvCount == maxMsgCount {
			kr.Close()
			close(done)
		}
	}

	kr = newKafRecv("inf-msg", msgHdl)
	<-done

	// Closed it after N messages without allowing the last commit.
	expectReaderCounters(t, maxMsgCount, maxMsgCount-1, 1)
}

func TestRecvReconnect(t *testing.T) {
	resetCounters()

	var kr *KafRecv
	recvCount := 0
	done := make(chan bool)
	msgHdl := func(key, val []byte) {
		expectMsg(t, recvCount%5, key, val) // a new reader after 5 msg
		recvCount++
		if recvCount == 7 {
			kr.Close()
			close(done)
		}
	}

	kr = newKafRecv("five-msg", msgHdl)
	<-done

	// Closed it after 7 messages without allowing the last commit.
	// There is also an extra failed fetch when the reader fails.
	// Reader failed after 5 and reconnected, thus the extra close.
	expectReaderCounters(t, 8, 6, 2)
}

func TestRecvBlock(t *testing.T) {
	resetCounters()
	msgHdl := func(key, val []byte) {
		t.Errorf("recv block got unexpected msg: (%s, %s)", string(key), string(val))
	}

	kr := newKafRecv("block", msgHdl)
	time.Sleep(500 * time.Millisecond)
	kr.Close()

	expectReaderCounters(t, 1, 0, 1)
}

func TestRecvNoCommit(t *testing.T) {
	resetCounters()
	msgHdl := func(key, val []byte) {
		expectMsg(t, 0, key, val) // only see the 1st message that fails to commit
	}

	kr := newKafRecv("nocommit", msgHdl)
	time.Sleep(1500 * time.Millisecond)
	kr.Close()

	// Reader failed the commit and reconnected, thus the extra close.
	expectReaderCounters(t, 1, 0, 2)
}

func TestSend(t *testing.T) {
	resetCounters()

	ks := newKafSend("five-msg")

	for i := 0; i < 7; i++ {
		key, val := mkMsg(i)
		if err := ks.SendMessage(key, val); err != nil {
			t.Errorf("cannot send message (%d): %v", i, err)
		}
	}

	time.Sleep(1500 * time.Millisecond)
	ks.Close()

	// All messages sent, plus one failed write that reconnected the
	// writer, closing the old one, thus an extra close.
	expectWriterCounters(t, 8, 2)
}

func TestSendBlock(t *testing.T) {
	resetCounters()

	ks := newKafSend("block")

	for i := 0; i < 7; i++ {
		key, val := mkMsg(i)
		if err := ks.SendMessage(key, val); err != nil {
			t.Errorf("cannot send message (%d): %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)
	ks.Close()

	key, val := mkMsg(999)
	if err := ks.SendMessage(key, val); err == nil {
		t.Errorf("message should not be accepted after Close")
	}

	// Writer got a message to send and blocked until closed.
	expectWriterCounters(t, 1, 1)
}
