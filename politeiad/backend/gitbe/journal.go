package gitbe

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

var (
	ErrBusy        = fmt.Errorf("busy")
	ErrNotFound    = fmt.Errorf("not found")
	ErrLineTooLong = fmt.Errorf("line too long")
)

type journalFile struct {
	file    *os.File
	scanner *bufio.Scanner
}

type Journal struct {
	sync.Mutex
	journals map[string]*journalFile
}

func NewJournal() *Journal {
	return &Journal{
		journals: make(map[string]*journalFile),
	}
}

func (j *Journal) Lock() {
	j.Mutex.Lock()
}

func (j *Journal) Unlock() {
	j.Mutex.Unlock()
}

func (j *Journal) Journal(filename, content string) error {
	j.Lock()
	defer j.Unlock()

	if _, ok := j.journals[filename]; ok {
		return ErrBusy
	}

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	defer f.Close()

	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	_, err = f.Write([]byte(content))
	return err
}

func (j *Journal) Open(filename string) error {
	j.Lock()
	defer j.Unlock()

	if _, ok := j.journals[filename]; ok {
		return ErrBusy
	}

	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	j.journals[filename] = &journalFile{
		file:    f,
		scanner: bufio.NewScanner(f),
	}

	return nil
}

func (j *Journal) Close(filename string) error {
	j.Lock()
	defer j.Unlock()

	f, ok := j.journals[filename]
	if !ok {
		return ErrNotFound
	}
	delete(j.journals, filename)
	return f.file.Close()
}

func (j *Journal) Replay(filename string, replay func(string) error) error {
	j.Lock()
	f, ok := j.journals[filename]
	if !ok {
		j.Unlock()
		return ErrNotFound
	}
	j.Unlock()

	// We can run unlocked from here

	if !f.scanner.Scan() {
		if f.scanner.Err() == nil {
			return io.EOF
		}
		return f.scanner.Err()
	}

	return replay(f.scanner.Text())
}
