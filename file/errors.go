package file

import "github.com/pkg/errors"

var (
	// ErrNoInput is returned when no input is provided.
	ErrNoInput = errors.New("no input provided")

	// ErrNoOutput is returned when no output is provided.
	ErrNoOutput = errors.New("no output provided")

	// ErrNoChunkFolder is returned when the chunk folder is not provided.
	ErrNoChunkFolder = errors.New("chunk folder is not provided")

	// ErrNoAllocator is returned when the allocator is not provided.
	ErrNoAllocator = errors.New("allocator is not provided")
)
