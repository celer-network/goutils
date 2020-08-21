// Copyright 2020 Celer Network
//
// API wrappers to handle receiving and sending Kafka events.
//
// KafRecv spawns a goroutine to receive Kafka events on a given topic
// and pass them to a callback function for processing.
//
// KafSend spawns a goroutine to send out Kafka events on a given topic.
//
// Both provide reliable interfaces that handle lost connectivity by
// reconnecting and retrying.

package kaf

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/celer-network/goutils/log"
	"github.com/segmentio/kafka-go"
)

const (
	maxRetryDelay = 30 // seconds
	sendChanSize  = 1024
)

var (
	ErrKafClosed = errors.New("kaf object is closed")
)

// Interface for the real Kafka Reader to allow mock for testing.
type KafReader interface {
	Close() error
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	FetchMessage(ctx context.Context) (kafka.Message, error)
}

// Interface for the real Kafka Writer to allow mock for testing.
type KafWriter interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type KafRecvFunc func(key, value []byte)
type KafReaderFunc func(config *kafka.ReaderConfig) KafReader
type KafWriterFunc func(config *kafka.WriterConfig) KafWriter

type KafRecv struct {
	topic    string
	config   *kafka.ReaderConfig
	ctx      context.Context
	cancel   context.CancelFunc
	mkReader KafReaderFunc
	cb       KafRecvFunc
	mu       sync.Mutex // protects mutables "closed" and "reader"
	closed   bool
	reader   KafReader
}

type KafSend struct {
	topic    string
	config   *kafka.WriterConfig
	ctx      context.Context
	cancel   context.CancelFunc
	ch       chan *kafka.Message
	mkWriter KafWriterFunc
	mu       sync.Mutex // protects mutables "closed" and "writer"
	closed   bool
	writer   KafWriter
}

// Create a KafRecv and start a goroutine to handle incoming messages on
// the given topic, passing each incoming message to the callback handler.
func NewKafRecv(kafkaUrl, component, topic string, cb KafRecvFunc) *KafRecv {
	// Constructor of real internal Kafka readers.
	mkReader := func(config *kafka.ReaderConfig) KafReader {
		return kafka.NewReader(*config)
	}

	return makeKafRecv(kafkaUrl, component, topic, cb, mkReader)
}

// Internal helper to create a KafRecv with a specified constructor of a Kafka reader.
// It allows unittests to be written passing it a constructor of mock Kafka readers.
func makeKafRecv(kafkaUrl, component, topic string, cb KafRecvFunc, mkReader KafReaderFunc) *KafRecv {
	component = strings.ToLower(component)
	topic = strings.ToLower(topic)

	config := &kafka.ReaderConfig{
		Brokers:     strings.Split(kafkaUrl, ","),
		Topic:       topic,
		GroupID:     component + "-" + topic,
		MinBytes:    100,  // 100 bytes
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
	}

	ctx, cancel := context.WithCancel(context.Background())

	kr := &KafRecv{
		topic:    topic,
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		mkReader: mkReader,
		cb:       cb,
	}

	go kr.handleRecvMessages()

	return kr
}

// Close and terminate the goroutine message handler.
func (kr *KafRecv) Close() {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if !kr.closed {
		kr.closed = true
		if kr.cancel != nil {
			kr.cancel() // let handler breakout of FetchMessage()
		}
		if kr.reader != nil {
			kr.reader.Close()
			kr.reader = nil
		}
	}
}

func (kr *KafRecv) handleRecvMessages() {
	logHdr := "handleRecvMessages: " + kr.topic + ":"
	for {
		log.Debugln(logHdr, "before fetchMessage")
		m, err := kr.fetchMessage()
		if err != nil {
			log.Warnln(logHdr, "exiting:", err)
			return
		}

		log.Debugln(logHdr, "p:", m.Partition, ", o:", m.Offset, ", t:", m.Time)
		kr.cb(m.Key, m.Value)
		kr.commitMessage(m)
	}
}

// Return the real internal Kafka reader, creating a new one if needed.
func (kr *KafRecv) getReader() (KafReader, error) {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return nil, ErrKafClosed
	}
	if kr.reader == nil {
		kr.reader = kr.mkReader(kr.config)
	}
	return kr.reader, nil
}

// Refresh the real internal Kafka reader, closing the old one if any.
func (kr *KafRecv) refresh() {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if !kr.closed {
		if kr.reader != nil {
			kr.reader.Close()
		}
		kr.reader = kr.mkReader(kr.config)
	}
}

func sleepBackoff(sec int) int {
	time.Sleep(time.Duration(sec) * time.Second)

	sec *= 2
	if sec > maxRetryDelay {
		sec = maxRetryDelay
	}
	return sec
}

// Wraps the FetchMessage() of the real internal Kafka reader.
// Handles connectivity failures by retrying using a new reader.
func (kr *KafRecv) fetchMessage() (*kafka.Message, error) {
	delay := 1

	for {
		r, err := kr.getReader()
		if err != nil {
			return nil, err // closed by app
		}

		msg, err := r.FetchMessage(kr.ctx)
		if err == nil {
			return &msg, nil // got a message
		}
		if kr.ctx.Err() == context.Canceled {
			return nil, kr.ctx.Err() // closed by app
		}

		log.Warnf("kafka reader (%s) fetch error -- retry: %v", kr.topic, err)
		delay = sleepBackoff(delay)
		kr.refresh()
	}
}

// Wraps the CommitMessages() of the real internal Kafka reader.
// Handles connectivity failures by retrying using a new reader.
func (kr *KafRecv) commitMessage(msg *kafka.Message) error {
	delay := 1

	for {
		r, err := kr.getReader()
		if err != nil {
			return err // closed by app
		}

		err = r.CommitMessages(kr.ctx, *msg)
		if err == nil {
			return nil // success
		}
		if kr.ctx.Err() == context.Canceled {
			return kr.ctx.Err() // closed by app
		}

		log.Warnf("kafka reader (%s) commit error -- retry: %v", kr.topic, err)
		delay = sleepBackoff(delay)
		kr.refresh()
	}
}

// Create a KafSend and start a goroutine to handle outgoing messages and react to
// connectivity failures.
func NewKafSend(kafkaUrl, topic string) *KafSend {
	// Constructor of real internal Kafka writers.
	mkWriter := func(config *kafka.WriterConfig) KafWriter {
		return kafka.NewWriter(*config)
	}

	return makeKafSend(kafkaUrl, topic, mkWriter)
}

// Internal helper to create a KafSend with a specified constructor of a Kafka writer.
// It allows unittests to be written passing it a constructor of mock Kafka writers.
func makeKafSend(kafkaUrl, topic string, mkWriter KafWriterFunc) *KafSend {
	topic = strings.ToLower(topic)

	config := &kafka.WriterConfig{
		Brokers:      strings.Split(kafkaUrl, ","),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())

	ks := &KafSend{
		topic:    topic,
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		ch:       make(chan *kafka.Message, sendChanSize),
		mkWriter: mkWriter,
	}

	go ks.handleSendMessages()

	return ks
}

// Close and terminate the goroutine message handler.
func (ks *KafSend) Close() {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if !ks.closed {
		ks.closed = true
		if ks.cancel != nil {
			ks.cancel() // let handler breakout of WriteMessages()
		}
		if ks.writer != nil {
			ks.writer.Close()
			ks.writer = nil
		}
	}
}

func (ks *KafSend) handleSendMessages() {
	logHdr := "handleSendMessages: " + ks.topic + ":"
	for {
		log.Debugln(logHdr, "before chan select")
		select {
		case <-ks.ctx.Done():
			log.Warnln(logHdr, "exiting:", ks.ctx.Err())
			return

		case msg, ok := <-ks.ch:
			if !ok {
				log.Errorln(logHdr, "send channel closed: exiting")
				return
			}

			// Write the message to Kafka reliably, or exit (app terminating).
			if err := ks.write(msg); err != nil {
				log.Warnln(logHdr, "exiting from write():", err)
				return
			}
		}
	}
}

// Return the real internal Kafka writer, creating a new one if needed.
func (ks *KafSend) getWriter() (KafWriter, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if ks.closed {
		return nil, ErrKafClosed
	}
	if ks.writer == nil {
		ks.writer = ks.mkWriter(ks.config)
	}
	return ks.writer, nil
}

// Refresh the real internal Kafka writer, closing the old one if any.
func (ks *KafSend) refresh() {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if !ks.closed {
		if ks.writer != nil {
			ks.writer.Close()
		}
		ks.writer = ks.mkWriter(ks.config)
	}
}

// Wraps the WriteMessages() of the real internal Kafka writer.
// Handles connectivity failures by retrying using a new writer.
func (ks *KafSend) write(msg *kafka.Message) error {
	delay := 1

	for {
		w, err := ks.getWriter()
		if err != nil {
			return err // closed by app
		}

		err = w.WriteMessages(ks.ctx, *msg)
		if err == nil {
			return nil // success
		}
		if ks.ctx.Err() == context.Canceled {
			return ks.ctx.Err() // closed by app
		}

		log.Warnf("kafka writer (%s): %s: error -- retry: %v", ks.topic, string(msg.Key), err)
		delay = sleepBackoff(delay)
		ks.refresh()
	}
}

// The API to pass a message to the goroutine sending outgoing messages.
func (ks *KafSend) SendMessage(key, val []byte) error {
	// Don't hold the mutex later when writing to the channel.
	ks.mu.Lock()
	closed := ks.closed
	ks.mu.Unlock()

	if closed {
		return ErrKafClosed
	}

	msg := &kafka.Message{
		Key:   key,
		Value: val,
	}
	ks.ch <- msg
	return nil
}
