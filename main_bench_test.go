package main_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkSort(b *testing.B) {
	filename := "test.tsv"
	chunkSize := 10000
	bufferSize := 5000
	f, err := os.Open(filename)
	assert.NoError(b, err)
	defer f.Close()

	output, err := os.Create("testdata/chunks/output.tsv")
	require.NoError(b, err)
	defer output.Close()

	fI := &file.Info{
		Input:       f,
		Allocate:    vector.DefaultVector(key.AllocateInt),
		Output:      output,
		ChunkFolder: "testdata/chunks",
	}
	err = fI.CreateSortedChunks(context.Background(), chunkSize, 100)
	assert.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = fI.MergeSort(bufferSize)
		_ = err
	}
	dir, err := ioutil.ReadDir("testdata/chunks")
	assert.NoError(b, err)
	for _, d := range dir {
		err = os.RemoveAll(path.Join("testdata/chunks", d.Name()))
		assert.NoError(b, err)
	}
}
