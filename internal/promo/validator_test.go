package promo

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type exactMembership map[string]bool

func (m exactMembership) PossiblyContains(_ context.Context, code string) bool { return m[code] }

var benchmarkResult bool

// TestValidatorPolicy checks format and every 2-of-3 membership combination.
func TestValidatorPolicy(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		datasets [3]bool
		want     bool
	}{
		{name: "none", code: "COUPON00", datasets: [3]bool{}, want: false},
		{name: "file 1 only", code: "COUPON00", datasets: [3]bool{true, false, false}, want: false},
		{name: "file 2 only", code: "COUPON00", datasets: [3]bool{false, true, false}, want: false},
		{name: "file 3 only", code: "COUPON00", datasets: [3]bool{false, false, true}, want: false},
		{name: "files 1 and 2", code: "COUPON00", datasets: [3]bool{true, true, false}, want: true},
		{name: "files 1 and 3", code: "COUPON00", datasets: [3]bool{true, false, true}, want: true},
		{name: "files 2 and 3", code: "COUPON00", datasets: [3]bool{false, true, true}, want: true},
		{name: "all files", code: "COUPON00", datasets: [3]bool{true, true, true}, want: true},
		{name: "too short", code: "SHORT", datasets: [3]bool{true, true, true}, want: false},
		{name: "too long", code: "LONGCOUPON11", datasets: [3]bool{true, true, true}, want: false},
		{name: "invalid characters", code: "BAD-CODE", datasets: [3]bool{true, true, true}, want: false},
		{name: "valid length 10", code: "COUPON0000", datasets: [3]bool{true, true, false}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			datasets := make([]Membership, 3)
			for i, present := range test.datasets {
				datasets[i] = exactMembership{test.code: present}
			}
			index := NewValidator(datasets...)
			if got := index.Validate(context.Background(), test.code); got != test.want {
				t.Fatalf("Validate(%q) = %v, want %v", test.code, got, test.want)
			}
		})
	}
}

// TestBuildSaveLoad verifies streaming parse, per-file deduplication, versioned
// persistence, and policy behavior after loading.
func TestBuildSaveLoad(t *testing.T) {
	inputDirectory := t.TempDir()
	inputs := []string{
		writeCoupons(t, inputDirectory, "couponbase1.gz", "HAPPYHRS", "HAPPYHRS", "SUPER100"),
		writeCoupons(t, inputDirectory, "couponbase2.gz", "xx HAPPYHRS yy", "FIFTYOFF"),
		writeCoupons(t, inputDirectory, "couponbase3.gz", "FIFTYOFF"),
	}
	outputRoot := t.TempDir()
	info, err := BuildVersion(context.Background(), inputs, outputRoot, "test-v1", BuildOptions{ChunkKeys: 2})
	if err != nil {
		t.Fatalf("BuildVersion() error = %v", err)
	}
	if info.UniqueKeys != 5 {
		t.Fatalf("unique keys across datasets = %d, want 5", info.UniqueKeys)
	}
	checker, loaded, err := LoadVersionWithLayers(
		filepath.Join(outputRoot, "test-v1"),
		nil,
		[]ExactMembership{
			ExactSet{"HAPPYHRS": {}, "SUPER100": {}},
			ExactSet{"HAPPYHRS": {}, "FIFTYOFF": {}},
			ExactSet{"FIFTYOFF": {}},
		},
	)
	if err != nil {
		t.Fatalf("LoadVersion() error = %v", err)
	}
	if loaded.Version != "test-v1" || loaded.DatasetCount != 3 {
		t.Fatalf("loaded info = %+v", loaded)
	}
	for code, want := range map[string]bool{
		"HAPPYHRS": true,
		"FIFTYOFF": true,
		"SUPER100": false,
	} {
		if got := checker.Validate(context.Background(), code); got != want {
			t.Fatalf("Validate(%q) = %v, want %v", code, got, want)
		}
	}
	manifestFile, err := os.Open(filepath.Join(outputRoot, "test-v1", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer manifestFile.Close()
	var manifest Manifest
	if err := json.NewDecoder(manifestFile).Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Datasets[0].Keys != 2 {
		t.Fatalf("duplicates in file 1 produced %d keys, want 2", manifest.Datasets[0].Keys)
	}
}

// TestPersistencePreservesMembership compares a filter before and after its
// native representation is saved and loaded.
func TestPersistencePreservesMembership(t *testing.T) {
	keys := []uint64{hashCode("COUPON01"), hashCode("COUPON02"), hashCode("COUPON03")}
	original, err := NewBinaryFuseIndex(keys)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "dataset.bin")
	size, checksum, err := saveDataset(path, original)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := loadDataset(directory, DatasetManifest{
		Name: "test", File: "dataset.bin", Keys: 3, Bytes: size, SHA256: checksum,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"COUPON01", "COUPON02", "COUPON03", "UNKNOWN1", "UNKNOWN2"} {
		if original.PossiblyContains(context.Background(), code) != loaded.PossiblyContains(context.Background(), code) {
			t.Fatalf("membership changed after reload for %q", code)
		}
	}
}

// TestCorruptFilterFailsLoad ensures startup cannot silently use a damaged index.
func TestCorruptFilterFailsLoad(t *testing.T) {
	directory := buildTestVersion(t)
	path := filepath.Join(directory, "couponbase1.bin")
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, nativeHeaderBytes); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if _, _, err := LoadVersion(directory); err == nil {
		t.Fatal("LoadVersion() succeeded with corrupt filter")
	}
}

func TestMissingInputsAndIndexesFailClearly(t *testing.T) {
	t.Run("missing builder input", func(t *testing.T) {
		inputs := []string{"missing-one.gz", "missing-two.gz", "missing-three.gz"}
		if _, err := BuildVersion(context.Background(), inputs, t.TempDir(), "v1", BuildOptions{}); err == nil {
			t.Fatal("BuildVersion() succeeded with missing input")
		}
	})
	t.Run("missing persisted filter", func(t *testing.T) {
		directory := buildTestVersion(t)
		if err := os.Remove(filepath.Join(directory, "couponbase2.bin")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadVersion(directory); err == nil {
			t.Fatal("LoadVersion() succeeded with missing filter")
		}
	})
	t.Run("unsupported manifest version", func(t *testing.T) {
		manifest := Manifest{
			FormatVersion:   IndexFormatVersion + 1,
			Version:         "future",
			HashAlgorithm:   hashAlgorithm,
			FingerprintBits: fingerprintBits,
			Datasets:        make([]DatasetManifest, expectedDatasets),
		}
		if err := validateManifest(manifest); err == nil {
			t.Fatal("validateManifest() accepted an unsupported format")
		}
	})
}

// TestLayeredMembership documents the possible-membership contract and shows
// how exact confirmation removes false-positive acceptance.
func TestLayeredMembership(t *testing.T) {
	possibleBase := exactMembership{"POSSIBLE": true}
	withoutExact := NewLayeredMembership(possibleBase, nil, nil)
	if !withoutExact.PossiblyContains(context.Background(), "POSSIBLE") {
		t.Fatal("possible base match was rejected")
	}
	withExact := NewLayeredMembership(possibleBase, nil, ExactSet{})
	if withExact.PossiblyContains(context.Background(), "POSSIBLE") {
		t.Fatal("exact confirmer did not reject possible false positive")
	}
	delta := MapDelta{"NEWCODE1": true, "POSSIBLE": false}
	layered := NewLayeredMembership(possibleBase, delta, nil)
	if !layered.PossiblyContains(context.Background(), "NEWCODE1") {
		t.Fatal("exact delta addition was rejected")
	}
	if layered.PossiblyContains(context.Background(), "POSSIBLE") {
		t.Fatal("exact delta revocation was ignored")
	}
}

// TestConcurrentMembership exercises immutable filter reads concurrently.
func TestConcurrentMembership(t *testing.T) {
	index, err := NewBinaryFuseIndex([]uint64{
		hashCode("COUPON01"),
		hashCode("COUPON02"),
		hashCode("COUPON03"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for iteration := 0; iteration < 1_000; iteration++ {
				if !index.PossiblyContains(context.Background(), "COUPON01") {
					t.Errorf("known key returned false")
					return
				}
			}
		}()
	}
	wait.Wait()
}

// TestFileEngine checks that validation and discount calculation stay separate.
func TestFileEngine(t *testing.T) {
	index := NewValidator(
		exactMembership{"HAPPYHRS": true, "SUPER100": true},
		exactMembership{"HAPPYHRS": true},
		exactMembership{},
	)
	engine := NewFileEngine(index, PercentOff(0.10))
	if got := engine.Discount(context.Background(), "HAPPYHRS", 1330); got != 133 {
		t.Fatalf("valid coupon discount = %d, want 133", got)
	}
	if got := engine.Discount(context.Background(), "SUPER100", 1330); got != 0 {
		t.Fatalf("invalid coupon discount = %d, want 0", got)
	}
	if got := engine.Discount(context.Background(), "", 1330); got != 0 {
		t.Fatalf("empty coupon discount = %d, want 0", got)
	}
}

func BenchmarkMembership(b *testing.B) {
	const keyCount = 100_000
	keys := make([]uint64, keyCount)
	exact := make(map[string]struct{}, keyCount)
	for i := range keys {
		code := fmt.Sprintf("C%09d", i)
		keys[i] = hashCode(code)
		exact[code] = struct{}{}
	}
	filter, err := NewBinaryFuseIndex(keys)
	if err != nil {
		b.Fatal(err)
	}
	code := fmt.Sprintf("C%09d", keyCount/2)
	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, benchmarkResult = exact[code]
		}
	})
	b.Run("binary_fuse_16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkResult = filter.PossiblyContains(context.Background(), code)
		}
	})
}

func BenchmarkValidateCoupon(b *testing.B) {
	code := "COUPON01"
	validator := NewValidator(
		exactMembership{code: true},
		exactMembership{code: true},
		exactMembership{},
	)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkResult = validator.Validate(context.Background(), code)
	}
}

func BenchmarkIndexBuild(b *testing.B) {
	const keyCount = 100_000
	codes := make([]string, keyCount)
	hashes := make([]uint64, keyCount)
	for i := range codes {
		codes[i] = fmt.Sprintf("C%09d", i)
		hashes[i] = hashCode(codes[i])
	}
	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			index := make(map[string]int, keyCount)
			for _, code := range codes {
				index[code] = 1
			}
			benchmarkResult = len(index) == keyCount
		}
	})
	b.Run("binary_fuse_16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			keys := append([]uint64(nil), hashes...)
			index, err := NewBinaryFuseIndex(keys)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkResult = index.PossiblyContains(context.Background(), codes[keyCount/2])
		}
	})
}

func BenchmarkIndexLoad(b *testing.B) {
	const keyCount = 100_000
	inputDirectory := b.TempDir()
	inputs := make([]string, expectedDatasets)
	for dataset := range inputs {
		path := filepath.Join(inputDirectory, fmt.Sprintf("couponbase%d.gz", dataset+1))
		file, err := os.Create(path)
		if err != nil {
			b.Fatal(err)
		}
		writer := gzip.NewWriter(file)
		for key := 0; key < keyCount; key++ {
			if _, err := fmt.Fprintf(writer, "C%09d\n", dataset*keyCount+key); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
		inputs[dataset] = path
	}
	outputRoot := b.TempDir()
	if _, err := BuildVersion(context.Background(), inputs, outputRoot, "benchmark", BuildOptions{ChunkKeys: 10_000}); err != nil {
		b.Fatal(err)
	}
	directory := filepath.Join(outputRoot, "benchmark")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		validator, _, err := LoadVersion(directory)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkResult = validator.Validate(context.Background(), "C000050000")
	}
}

func buildTestVersion(t *testing.T) string {
	t.Helper()
	inputDirectory := t.TempDir()
	inputs := []string{
		writeCoupons(t, inputDirectory, "couponbase1.gz", "COUPON01"),
		writeCoupons(t, inputDirectory, "couponbase2.gz", "COUPON01"),
		writeCoupons(t, inputDirectory, "couponbase3.gz", "COUPON02"),
	}
	outputRoot := t.TempDir()
	if _, err := BuildVersion(context.Background(), inputs, outputRoot, "v1", BuildOptions{ChunkKeys: 2}); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(outputRoot, "v1")
}

// writeCoupons writes a gzip coupon dump containing the given lines.
func writeCoupons(t *testing.T, directory, name string, lines ...string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create coupon file: %v", err)
	}
	writer := gzip.NewWriter(file)
	for _, line := range lines {
		if _, err := writer.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write coupon: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close coupon file: %v", err)
	}
	return path
}
