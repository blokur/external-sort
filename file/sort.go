package file

import (
	"bufio"

	"github.com/askiada/external-sort/vector"
	"github.com/cheggaaa/pb/v3"
	"github.com/pkg/errors"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// MergeSort sorts the file from it's chunks.
func (i *Info) MergeSort(k int) error {
	output := i.Allocate.Vector(k, i.Allocate.Key)
	chunks := &chunks{list: make([]*chunkInfo, 0, len(i.chunkPaths))}
	for _, chunkPath := range i.chunkPaths {
		err := chunks.new(chunkPath, i.Allocate, k)
		if err != nil {
			return errors.Wrap(err, "failed to create chunk")
		}
	}

	outputBuffer := bufio.NewWriter(i.Output)

	bar := pb.StartNew(i.totalRows)
	chunks.resetOrder()
	for chunks.len() > 0 {
		if output.Len() == k {
			err := WriteBuffer(outputBuffer, output)
			if err != nil {
				return errors.Wrap(err, "failed to write buffer")
			}
		}
		toShrink := []int{}
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minValue, minIdx := chunks.min()
		err := output.PushBack(minValue.Line)
		if err != nil {
			return errors.Wrap(err, "failed to push back to output")
		}
		// remove the first element from the chunk we pulled the smallest value
		minChunk.buffer.FrontShift()
		isEmpty := false
		if minChunk.buffer.Len() == 0 {
			err = minChunk.pullSubset(k)
			if err != nil {
				return errors.Wrap(err, "failed to pull subset")
			}
			// if after pulling data the chunk buffer is still empty then we can remove it
			if minChunk.buffer.Len() == 0 {
				isEmpty = true
				toShrink = append(toShrink, minIdx)
				err = chunks.shrink(toShrink)
				if err != nil {
					return errors.Wrap(err, "failed to shrink chunks")
				}
			}
		}
		// when we get a new element in the first chunk we need to re-order it
		if !isEmpty {
			chunks.moveFirstChunkToCorrectIndex()
		}
		bar.Increment()
	}

	err := WriteBuffer(outputBuffer, output)
	if err != nil {
		return errors.Wrap(err, "failed to write buffer")
	}

	err = outputBuffer.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush output buffer")
	}
	bar.Finish()
	return chunks.close()
}

func WriteBuffer(buffer *bufio.Writer, rows vector.Vector) error {
	for i := 0; i < rows.Len(); i++ {
		_, err := buffer.WriteString(rows.Get(i).Line + "\n")
		if err != nil {
			return err
		}
	}
	rows.Reset()
	return nil
}
