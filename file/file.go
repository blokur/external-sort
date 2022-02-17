// Package file contains external sort algorithm for large files on disk.
package file

import (
	"bufio"
	"context"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/askiada/external-sort/file/batchingchannels"
	"github.com/askiada/external-sort/vector"
	"github.com/pkg/errors"
)

type Info struct {
	Input       io.Reader
	Output      io.Writer
	ChunkFolder string
	Allocate    *vector.Allocate
	totalRows   int
	chunkPaths  []string
}

// Sort sorts the file on disk using external sort algorithm.
func (i *Info) Sort(ctx context.Context, dumpSize, workers, size int) error {
	if i.Input == nil {
		return ErrNoInput
	}
	if i.Output == nil {
		return ErrNoOutput
	}
	if i.ChunkFolder == "" {
		return ErrNoChunkFolder
	}
	if i.Allocate == nil {
		return ErrNoAllocator
	}

	err := i.CreateSortedChunks(ctx, dumpSize, int64(workers))
	if err != nil {
		return errors.Wrap(err, "creating chunks")
	}
	return i.MergeSort(size)
}

// CreateSortedChunks Scan a file and divide it into small sorted chunks. It
// returns an error if it can't create chunkFolder.
func (i *Info) CreateSortedChunks(ctx context.Context, dumpSize int, maxWorkers int64) error {
	if dumpSize <= 0 {
		return errors.New("dump size must be greater than 0")
	}

	err := clearChunkFolder(i.ChunkFolder)
	if err != nil {
		return errors.Wrap(err, "cleaning chunk folder")
	}
	row := 0
	scanner := bufio.NewScanner(i.Input)
	mu := sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	batchChan, err := batchingchannels.NewBatchingChannel(ctx, i.Allocate, maxWorkers, dumpSize)
	if err != nil {
		return errors.Wrap(err, "creating batching channel")
	}
	go func() {
		defer wg.Done()
		for scanner.Scan() {
			text := scanner.Text()
			batchChan.In() <- text
			row++
		}
		batchChan.Close()
	}()

	chunkIdx := 0
	err = batchChan.ProcessOut(func(v vector.Vector) error {
		mu.Lock()
		chunkIdx++
		chunkPath := path.Join(i.ChunkFolder, "chunk_"+strconv.Itoa(chunkIdx)+".tsv")
		mu.Unlock()
		v.Sort()
		err := vector.Dump(v, chunkPath)
		if err != nil {
			return errors.Wrap(err, "dumping vector")
		}
		mu.Lock()
		i.chunkPaths = append(i.chunkPaths, chunkPath)
		mu.Unlock()
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "creating chunks")
	}
	wg.Wait()
	if scanner.Err() != nil {
		return errors.Wrap(scanner.Err(), "error while scanning")
	}
	i.totalRows = row
	return nil
}
