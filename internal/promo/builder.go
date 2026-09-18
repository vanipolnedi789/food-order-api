package promo

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

const (
	defaultChunkKeys = 1_000_000
	scannerMaxToken  = 1 << 20
)

// BuildOptions bounds parser memory independently of the input size.
type BuildOptions struct {
	ChunkKeys int
}

// BuildInfo reports offline construction costs without coupon values.
type BuildInfo struct {
	Version      string
	DatasetCount int
	UniqueKeys   uint64
	FilterBytes  int64
	InputBytes   int64
	Duration     time.Duration
}

// BuildVersion creates three BinaryFuse16 files and atomically publishes one
// version directory. Datasets are built sequentially to avoid multiplying
// xorfilter's substantial temporary construction memory.
func BuildVersion(ctx context.Context, inputs []string, outputRoot, version string, options BuildOptions) (BuildInfo, error) {
	started := time.Now()
	if len(inputs) != expectedDatasets {
		return BuildInfo{}, fmt.Errorf("build coupon index: got %d inputs, want %d", len(inputs), expectedDatasets)
	}
	if version == "" || version == "." || version == ".." || filepath.Base(version) != version {
		return BuildInfo{}, fmt.Errorf("build coupon index: invalid version %q", version)
	}
	if options.ChunkKeys <= 0 {
		options.ChunkKeys = defaultChunkKeys
	}
	if err := os.MkdirAll(outputRoot, 0o755); err != nil {
		return BuildInfo{}, fmt.Errorf("create index root: %w", err)
	}
	finalDirectory := filepath.Join(outputRoot, version)
	if _, err := os.Stat(finalDirectory); err == nil {
		return BuildInfo{}, fmt.Errorf("build coupon index: version %q already exists", version)
	} else if !errors.Is(err, os.ErrNotExist) {
		return BuildInfo{}, fmt.Errorf("inspect output version: %w", err)
	}
	tempDirectory, err := os.MkdirTemp(outputRoot, "."+version+".tmp-")
	if err != nil {
		return BuildInfo{}, fmt.Errorf("create temporary version directory: %w", err)
	}
	defer os.RemoveAll(tempDirectory)

	manifest := Manifest{
		FormatVersion:   IndexFormatVersion,
		Version:         version,
		CreatedAt:       time.Now().UTC(),
		HashAlgorithm:   hashAlgorithm,
		FingerprintBits: fingerprintBits,
		Datasets:        make([]DatasetManifest, 0, expectedDatasets),
	}
	var info BuildInfo
	for position, input := range inputs {
		if err := ctx.Err(); err != nil {
			return BuildInfo{}, err
		}
		keys, inputBytes, err := collectUniqueHashes(ctx, input, tempDirectory, options.ChunkKeys)
		if err != nil {
			return BuildInfo{}, fmt.Errorf("build dataset %d from %s: %w", position+1, input, err)
		}
		if len(keys) == 0 {
			return BuildInfo{}, fmt.Errorf("build dataset %d from %s: no valid coupons", position+1, input)
		}
		if uint64(len(keys)) > math.MaxUint32 {
			return BuildInfo{}, fmt.Errorf("build dataset %d: %d keys exceed Binary Fuse limit", position+1, len(keys))
		}
		keyCount := uint64(len(keys))
		index, err := NewBinaryFuseIndex(keys)
		keys = nil
		if err != nil {
			return BuildInfo{}, fmt.Errorf("construct BinaryFuse16 dataset %d: %w", position+1, err)
		}
		filename := fmt.Sprintf("couponbase%d.bin", position+1)
		size, checksum, err := saveDataset(filepath.Join(tempDirectory, filename), index)
		index = nil
		if err != nil {
			return BuildInfo{}, fmt.Errorf("save dataset %d: %w", position+1, err)
		}
		manifest.Datasets = append(manifest.Datasets, DatasetManifest{
			Name:       fmt.Sprintf("couponbase%d", position+1),
			Source:     filepath.Base(input),
			File:       filename,
			Keys:       keyCount,
			Bytes:      size,
			SHA256:     checksum,
			InputBytes: inputBytes,
		})
		info.UniqueKeys += keyCount
		info.FilterBytes += size
		info.InputBytes += inputBytes
		if position+1 < len(inputs) {
			// Construction arrays are much larger than the saved filter. Force
			// reclamation between sequential datasets so the next build cannot
			// temporarily retain both datasets' working sets.
			runtime.GC()
		}
	}
	if err := writeManifest(filepath.Join(tempDirectory, "manifest.json"), manifest); err != nil {
		return BuildInfo{}, fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tempDirectory, finalDirectory); err != nil {
		return BuildInfo{}, fmt.Errorf("publish index version %q: %w", version, err)
	}
	info.Version = version
	info.DatasetCount = expectedDatasets
	info.Duration = time.Since(started)
	return info, nil
}

func writeManifest(path string, manifest Manifest) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func collectUniqueHashes(ctx context.Context, input, tempDirectory string, chunkLimit int) ([]uint64, int64, error) {
	file, err := os.Open(input)
	if err != nil {
		return nil, 0, fmt.Errorf("open coupon file: %w", err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat coupon file: %w", err)
	}
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, 0, fmt.Errorf("create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	chunk := make([]uint64, 0, chunkLimit)
	var chunkPaths []string
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.Slice(chunk, func(i, j int) bool { return chunk[i] < chunk[j] })
		chunk = deduplicateSorted(chunk)
		path := filepath.Join(tempDirectory, fmt.Sprintf(".hashes-%d-%d", os.Getpid(), len(chunkPaths)))
		if err := writeHashChunk(path, chunk); err != nil {
			return err
		}
		chunkPaths = append(chunkPaths, path)
		chunk = make([]uint64, 0, chunkLimit)
		return nil
	}

	scanner := bufio.NewScanner(gzipReader)
	scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxToken)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if err := collectCodes(scanner.Text(), func(code string) error {
			chunk = append(chunk, hashCode(code))
			if len(chunk) >= chunkLimit {
				return flush()
			}
			return nil
		}); err != nil {
			return nil, 0, err
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("read coupon file: %w", err)
	}
	if err := flush(); err != nil {
		return nil, 0, err
	}
	defer func() {
		for _, path := range chunkPaths {
			_ = os.Remove(path)
		}
	}()
	keys, err := mergeHashChunks(ctx, chunkPaths)
	return keys, stat.Size(), err
}

func deduplicateSorted(values []uint64) []uint64 {
	if len(values) < 2 {
		return values
	}
	output := 1
	for _, value := range values[1:] {
		if value != values[output-1] {
			values[output] = value
			output++
		}
	}
	return values[:output]
}

func writeHashChunk(path string, values []uint64) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 256*1024)
	for _, value := range values {
		if err := binary.Write(writer, binary.LittleEndian, value); err != nil {
			file.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

type hashCursor struct {
	value  uint64
	reader *bufio.Reader
	file   *os.File
}

type cursorHeap []*hashCursor

func (h cursorHeap) Len() int           { return len(h) }
func (h cursorHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h cursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(value any)    { *h = append(*h, value.(*hashCursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func mergeHashChunks(ctx context.Context, paths []string) ([]uint64, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	cursors := make(cursorHeap, 0, len(paths))
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			closeCursors(cursors)
			return nil, err
		}
		cursor := &hashCursor{reader: bufio.NewReaderSize(file, 256*1024), file: file}
		if err := binary.Read(cursor.reader, binary.LittleEndian, &cursor.value); err != nil {
			file.Close()
			closeCursors(cursors)
			return nil, err
		}
		cursors = append(cursors, cursor)
	}
	merged, err := os.CreateTemp(filepath.Dir(paths[0]), ".merged-hashes-")
	if err != nil {
		closeCursors(cursors)
		return nil, err
	}
	defer os.Remove(merged.Name())
	writer := bufio.NewWriterSize(merged, 256*1024)
	heap.Init(&cursors)
	var previous uint64
	havePrevious := false
	var count uint64
	var processed uint64
	for cursors.Len() > 0 {
		if processed&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				closeCursors(cursors)
				merged.Close()
				return nil, err
			}
		}
		cursor := heap.Pop(&cursors).(*hashCursor)
		if !havePrevious || cursor.value != previous {
			if err := binary.Write(writer, binary.LittleEndian, cursor.value); err != nil {
				cursor.file.Close()
				closeCursors(cursors)
				merged.Close()
				return nil, err
			}
			previous = cursor.value
			havePrevious = true
			count++
		}
		processed++
		err := binary.Read(cursor.reader, binary.LittleEndian, &cursor.value)
		switch {
		case err == nil:
			heap.Push(&cursors, cursor)
		case errors.Is(err, io.EOF):
			cursor.file.Close()
		default:
			cursor.file.Close()
			closeCursors(cursors)
			merged.Close()
			return nil, err
		}
	}
	if err := writer.Flush(); err != nil {
		merged.Close()
		return nil, err
	}
	if count > uint64(math.MaxInt) {
		merged.Close()
		return nil, fmt.Errorf("too many unique hashes: %d", count)
	}
	if _, err := merged.Seek(0, io.SeekStart); err != nil {
		merged.Close()
		return nil, err
	}
	keys := make([]uint64, int(count))
	if err := binary.Read(bufio.NewReaderSize(merged, 256*1024), binary.LittleEndian, keys); err != nil {
		merged.Close()
		return nil, err
	}
	if err := merged.Close(); err != nil {
		return nil, err
	}
	return keys, nil
}

func closeCursors(cursors cursorHeap) {
	for _, cursor := range cursors {
		_ = cursor.file.Close()
	}
}
