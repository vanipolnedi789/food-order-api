package promo

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/FastFilter/xorfilter"
)

const (
	// IndexFormatVersion changes when the manifest or on-disk contract changes.
	IndexFormatVersion = 1
	expectedDatasets   = 3
	fingerprintBits    = 16
	hashAlgorithm      = "xxhash64"
	nativeHeaderBytes  = 28
)

// Manifest identifies one atomic, internally consistent coupon index version.
type Manifest struct {
	FormatVersion   int               `json:"formatVersion"`
	Version         string            `json:"version"`
	CreatedAt       time.Time         `json:"createdAt"`
	HashAlgorithm   string            `json:"hashAlgorithm"`
	FingerprintBits int               `json:"fingerprintBits"`
	Datasets        []DatasetManifest `json:"datasets"`
}

// DatasetManifest records integrity and sizing metadata for one source.
type DatasetManifest struct {
	Name       string `json:"name"`
	Source     string `json:"source"`
	File       string `json:"file"`
	Keys       uint64 `json:"keys"`
	Bytes      int64  `json:"bytes"`
	SHA256     string `json:"sha256"`
	InputBytes int64  `json:"inputBytes"`
}

// LoadInfo is safe startup observability; it contains no coupon values.
type LoadInfo struct {
	Version      string
	DatasetCount int
	FilterBytes  int64
	Keys         uint64
	Duration     time.Duration
}

// LoadVersion loads all datasets named by one manifest. It never mixes
// versions and never falls back to an empty index or source-file rebuild.
func LoadVersion(directory string) (*Validator, LoadInfo, error) {
	return LoadVersionWithLayers(directory, nil, nil)
}

// LoadVersionWithLayers loads a base version with optional exact delta and
// confirmation adapters, one per dataset.
func LoadVersionWithLayers(directory string, deltas []DeltaStore, confirmers []ExactMembership) (*Validator, LoadInfo, error) {
	started := time.Now()
	manifest, err := readManifest(directory)
	if err != nil {
		return nil, LoadInfo{}, err
	}
	if len(deltas) != 0 && len(deltas) != len(manifest.Datasets) {
		return nil, LoadInfo{}, fmt.Errorf("load coupon index: got %d delta stores for %d datasets", len(deltas), len(manifest.Datasets))
	}
	if len(confirmers) != 0 && len(confirmers) != len(manifest.Datasets) {
		return nil, LoadInfo{}, fmt.Errorf("load coupon index: got %d exact confirmers for %d datasets", len(confirmers), len(manifest.Datasets))
	}

	datasets := make([]Membership, 0, len(manifest.Datasets))
	var totalBytes int64
	var totalKeys uint64
	for position, metadata := range manifest.Datasets {
		index, err := loadDataset(directory, metadata)
		if err != nil {
			return nil, LoadInfo{}, fmt.Errorf("load coupon index version %q dataset %q: %w", manifest.Version, metadata.Name, err)
		}
		var delta DeltaStore
		if len(deltas) != 0 {
			delta = deltas[position]
		}
		var confirmer ExactMembership
		if len(confirmers) != 0 {
			confirmer = confirmers[position]
		}
		datasets = append(datasets, NewLayeredMembership(index, delta, confirmer))
		totalBytes += metadata.Bytes
		totalKeys += metadata.Keys
	}
	return NewValidator(datasets...), LoadInfo{
		Version:      manifest.Version,
		DatasetCount: len(datasets),
		FilterBytes:  totalBytes,
		Keys:         totalKeys,
		Duration:     time.Since(started),
	}, nil
}

func readManifest(directory string) (Manifest, error) {
	path := filepath.Join(directory, "manifest.json")
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("open manifest %s: %w", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest %s: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Manifest{}, fmt.Errorf("decode manifest %s: trailing content", path)
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, fmt.Errorf("invalid manifest %s: %w", path, err)
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.FormatVersion != IndexFormatVersion {
		return fmt.Errorf("unsupported format version %d", manifest.FormatVersion)
	}
	if manifest.Version == "" {
		return errors.New("version is required")
	}
	if manifest.HashAlgorithm != hashAlgorithm {
		return fmt.Errorf("unsupported hash algorithm %q", manifest.HashAlgorithm)
	}
	if manifest.FingerprintBits != fingerprintBits {
		return fmt.Errorf("unsupported fingerprint width %d", manifest.FingerprintBits)
	}
	if len(manifest.Datasets) != expectedDatasets {
		return fmt.Errorf("got %d datasets, want %d", len(manifest.Datasets), expectedDatasets)
	}
	names := make(map[string]struct{}, expectedDatasets)
	for _, dataset := range manifest.Datasets {
		if dataset.Name == "" || dataset.File == "" || dataset.Keys == 0 || dataset.Bytes <= nativeHeaderBytes {
			return fmt.Errorf("dataset %q has incomplete metadata", dataset.Name)
		}
		if filepath.Base(dataset.File) != dataset.File {
			return fmt.Errorf("dataset %q has unsafe file path %q", dataset.Name, dataset.File)
		}
		if len(dataset.SHA256) != sha256.Size*2 {
			return fmt.Errorf("dataset %q has invalid SHA-256", dataset.Name)
		}
		if _, exists := names[dataset.Name]; exists {
			return fmt.Errorf("duplicate dataset name %q", dataset.Name)
		}
		names[dataset.Name] = struct{}{}
	}
	return nil
}

func loadDataset(directory string, metadata DatasetManifest) (*BinaryFuseIndex, error) {
	path := filepath.Join(directory, metadata.File)
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open filter %s: %w", path, err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat filter %s: %w", path, err)
	}
	if stat.Size() != metadata.Bytes {
		return nil, fmt.Errorf("filter size is %d bytes, manifest requires %d", stat.Size(), metadata.Bytes)
	}
	if err := validateNativeSize(file, stat.Size()); err != nil {
		return nil, err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return nil, fmt.Errorf("checksum filter: %w", err)
	}
	if got := hex.EncodeToString(digest.Sum(nil)); got != metadata.SHA256 {
		return nil, fmt.Errorf("filter checksum mismatch")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind filter: %w", err)
	}
	filter, err := xorfilter.LoadBinaryFuse[uint16](file)
	if err != nil {
		return nil, fmt.Errorf("decode BinaryFuse16: %w", err)
	}
	if err := validateFilter(filter); err != nil {
		return nil, fmt.Errorf("invalid BinaryFuse16 structure: %w", err)
	}
	return &BinaryFuseIndex{filter: filter}, nil
}

// validateNativeSize prevents a corrupt length field from causing a huge
// allocation inside xorfilter's loader.
func validateNativeSize(file *os.File, size int64) error {
	if size <= nativeHeaderBytes || (size-nativeHeaderBytes)%2 != 0 {
		return fmt.Errorf("invalid BinaryFuse16 file size %d", size)
	}
	var lengthBytes [4]byte
	if _, err := file.ReadAt(lengthBytes[:], 24); err != nil {
		return fmt.Errorf("read BinaryFuse16 fingerprint length: %w", err)
	}
	fingerprints := int64(binary.LittleEndian.Uint32(lengthBytes[:]))
	if nativeHeaderBytes+fingerprints*2 != size {
		return fmt.Errorf("BinaryFuse16 fingerprint length does not match file size")
	}
	return nil
}

func validateFilter(filter *xorfilter.BinaryFuse[uint16]) error {
	if filter.SegmentLength == 0 || filter.SegmentLength&(filter.SegmentLength-1) != 0 {
		return errors.New("segment length is not a power of two")
	}
	if filter.SegmentLengthMask != filter.SegmentLength-1 || filter.SegmentCount == 0 {
		return errors.New("segment metadata is inconsistent")
	}
	if uint64(filter.SegmentCount)*uint64(filter.SegmentLength) != uint64(filter.SegmentCountLength) {
		return errors.New("segment count length is inconsistent")
	}
	expected := (uint64(filter.SegmentCount) + 2) * uint64(filter.SegmentLength)
	if expected != uint64(len(filter.Fingerprints)) {
		return errors.New("fingerprint count is inconsistent")
	}
	return nil
}

func saveDataset(path string, index *BinaryFuseIndex) (int64, string, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, "", err
	}
	hash := sha256.New()
	writer := io.MultiWriter(file, hash)
	if err := index.filter.Save(writer); err != nil {
		file.Close()
		return 0, "", err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return 0, "", err
	}
	if err := file.Close(); err != nil {
		return 0, "", err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return 0, "", err
	}
	return stat.Size(), hex.EncodeToString(hash.Sum(nil)), nil
}
