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

// Info sorts the Input with the external-sort algorithm and writes the sorted
// results into the Output. It requires the ChunkFolder for creating temporary
// files in order to reduce the memory size. In order to sort the Input you
// only need to call the Sort method. If you want a fine-grained control over
// the chunks, you can call the CreateSortedChunks and follow by a MergeSort
// call.
type Info struct {
	Input       io.Reader
	Output      io.Writer
	ChunkFolder string
	Allocate    *vector.Allocate
	totalRows   int
	chunkPaths  []string
}

// Sort sorts the file on disk using external sort algorithm. It returns an
// error if any of the exported properties of the Info is not provided, or an
// error during the operation occurred. The bufferSize is the amount of buffer
// we keep in memory per each chunk file to avoid loading the entire chunk when
// merging. Each chunk contains at most chunkSize lines.
func (i *Info) Sort(ctx context.Context, chunkSize, workers, bufferSize int) error {
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

	err := i.CreateSortedChunks(ctx, chunkSize, int64(workers))
	if err != nil {
		return errors.Wrap(err, "creating chunks")
	}
	return i.MergeSort(bufferSize)
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
		return errors.Wrap(err, "processing batches")
	}
	wg.Wait()
	if scanner.Err() != nil {
		return errors.Wrap(scanner.Err(), "error while scanning")
	}
	i.totalRows = row
	return nil
}
