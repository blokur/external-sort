package main_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/askiada/external-sort/file"
	"github.com/askiada/external-sort/vector"
	"github.com/askiada/external-sort/vector/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareChunks(ctx context.Context, t *testing.T, allocate *vector.Allocate, filename string, chunkSize int) *file.Info {
	t.Helper()
	f, err := os.Open(filename)
	require.NoError(t, err)

	tmp, err := ioutil.TempDir("", "external-sort")
	require.NoError(t, err)

	output, err := ioutil.TempFile(tmp, "")
	require.NoError(t, err)

	fI := &file.Info{
		Input:       f,
		Allocate:    allocate,
		Output:      output,
		ChunkFolder: path.Join(tmp, "testdata/chunks"),
	}
	err = fI.CreateSortedChunks(ctx, chunkSize, 10)
	require.NoError(t, err)

	t.Cleanup(func() {
		defer f.Close()
		output.Close()
		err = os.RemoveAll(tmp)
		require.NoError(t, err)
	})

	return fI
}

func TestBasics(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		expectedErr    error
		expectedOutput []string
	}{
		"empty file": {
			filename: "testdata/emptyfile.tsv",
		},
		"one elem": {
			filename:       "testdata/oneelem.tsv",
			expectedOutput: []string{"1"},
		},
		"100 elems": {
			filename:       "testdata/100elems.tsv",
			expectedOutput: []string{"3", "4", "5", "6", "6", "7", "7", "7", "8", "8", "9", "9", "10", "10", "15", "18", "18", "18", "18", "21", "22", "22", "25", "25", "25", "25", "25", "26", "26", "27", "27", "28", "28", "29", "29", "29", "30", "30", "31", "31", "33", "33", "34", "36", "37", "39", "39", "39", "40", "41", "41", "42", "43", "43", "47", "47", "49", "50", "50", "52", "52", "53", "54", "55", "55", "55", "56", "57", "57", "59", "60", "61", "62", "63", "67", "71", "71", "72", "72", "73", "74", "75", "78", "79", "80", "80", "82", "89", "89", "89", "91", "91", "92", "92", "93", "93", "94", "97", "97", "99"},
		},
	}
	allocate := vector.DefaultVector(key.AllocateInt)
	ctx := context.Background()
	for name, tc := range tcs {
		tc := tc
		for chunkSize := 1; chunkSize < 152; chunkSize += 10 {
			for bufferSize := 1; bufferSize < 152; bufferSize += 10 {
				chunkSize := chunkSize
				bufferSize := bufferSize
				t.Run(name+"_"+strconv.Itoa(chunkSize)+"_"+strconv.Itoa(bufferSize), func(t *testing.T) {
					fI := prepareChunks(ctx, t, allocate, tc.filename, chunkSize)
					output := fI.Output
					fI.Output = output
					err := fI.MergeSort(bufferSize)
					assert.NoError(t, err)
					outputFile := fI.Output.(*os.File)
					outputFile.Seek(0, io.SeekStart)
					outputScanner := bufio.NewScanner(outputFile)
					count := 0
					for outputScanner.Scan() {
						assert.Equal(t, tc.expectedOutput[count], outputScanner.Text())
						count++
					}
					assert.NoError(t, outputScanner.Err())
					assert.Equal(t, len(tc.expectedOutput), count)
					assert.True(t, errors.Is(err, tc.expectedErr))
				})
			}
		}
	}
}

func Test100Elems(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		expectedErr    error
		expectedOutput []string
	}{
		"100 elems": {
			filename:       "testdata/100elems.tsv",
			expectedOutput: []string{"3", "4", "5", "6", "6", "7", "7", "7", "8", "8", "9", "9", "10", "10", "15", "18", "18", "18", "18", "21", "22", "22", "25", "25", "25", "25", "25", "26", "26", "27", "27", "28", "28", "29", "29", "29", "30", "30", "31", "31", "33", "33", "34", "36", "37", "39", "39", "39", "40", "41", "41", "42", "43", "43", "47", "47", "49", "50", "50", "52", "52", "53", "54", "55", "55", "55", "56", "57", "57", "59", "60", "61", "62", "63", "67", "71", "71", "72", "72", "73", "74", "75", "78", "79", "80", "80", "82", "89", "89", "89", "91", "91", "92", "92", "93", "93", "94", "97", "97", "99"},
		},
	}
	allocate := vector.DefaultVector(key.AllocateInt)
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fI := prepareChunks(ctx, t, allocate, tc.filename, 21)
			err := fI.MergeSort(10)
			assert.NoError(t, err)
			outputFile := fI.Output.(*os.File)
			outputFile.Seek(0, io.SeekStart)
			outputScanner := bufio.NewScanner(outputFile)
			count := 0
			for outputScanner.Scan() {
				assert.Equal(t, tc.expectedOutput[count], outputScanner.Text())
				count++
			}
			assert.NoError(t, outputScanner.Err())
			assert.Equal(t, len(tc.expectedOutput), count)
			assert.True(t, errors.Is(err, tc.expectedErr))
		})
	}
}

func TestTsvKey(t *testing.T) {
	tcs := map[string]struct {
		filename       string
		expectedErr    error
		expectedOutput []string
	}{
		"Tsv file": {
			filename: "testdata/multifields.tsv",
			expectedOutput: []string{"3	D	equipment",
				"7	G	inflation",
				"6	H	delivery",
				"9	I	child",
				"5	J	magazine",
				"8	M	garbage",
				"1	N	guidance",
				"10	S	feedback",
				"2	T	library",
				"4	Z	news"},
		},
	}
	allocate := vector.DefaultVector(func(line string) (key.Key, error) {
		return key.AllocateTsv(line, 1)
	})
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fI := prepareChunks(ctx, t, allocate, tc.filename, 21)
			err := fI.MergeSort(10)
			assert.NoError(t, err)
			outputFile := fI.Output.(*os.File)
			outputFile.Seek(0, io.SeekStart)
			outputScanner := bufio.NewScanner(outputFile)
			count := 0
			for outputScanner.Scan() {
				assert.Equal(t, tc.expectedOutput[count], outputScanner.Text())
				count++
			}
			assert.NoError(t, outputScanner.Err())
			assert.Equal(t, len(tc.expectedOutput), count)
			assert.True(t, errors.Is(err, tc.expectedErr))
		})
	}
}
