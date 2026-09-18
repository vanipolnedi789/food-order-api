package promo

import (
	"context"

	"github.com/FastFilter/xorfilter"
	"github.com/cespare/xxhash/v2"
)

// BinaryFuseIndex is an immutable BinaryFuse16 membership index.
//
// BinaryFuse16 uses about 18 bits/key at scale and has an approximately
// 0.0015% false-positive rate. Contains calls are read-only and safe to run
// concurrently without locks.
type BinaryFuseIndex struct {
	filter *xorfilter.BinaryFuse[uint16]
}

// NewBinaryFuseIndex builds a membership index from already deduplicated
// 64-bit hashes. Building is intentionally used by the offline builder, not
// the API startup path.
func NewBinaryFuseIndex(keys []uint64) (*BinaryFuseIndex, error) {
	filter, err := xorfilter.NewBinaryFuse[uint16](keys)
	if err != nil {
		return nil, err
	}
	return &BinaryFuseIndex{filter: filter}, nil
}

// PossiblyContains reports probabilistic membership. True means "possibly
// present"; false means "definitely absent".
func (i *BinaryFuseIndex) PossiblyContains(_ context.Context, code string) bool {
	return i.filter.Contains(hashCode(code))
}

func hashCode(code string) uint64 {
	return xxhash.Sum64String(code)
}

// ExactMembership can confirm a possible base match against an authoritative
// source. A DB, Redis, or remote campaign adapter can implement this later.
type ExactMembership interface {
	Contains(ctx context.Context, code string) bool
}

// DeltaStore contains exact overrides for coupons changed since the immutable
// base was built. found=true with active=false represents a revocation.
type DeltaStore interface {
	Lookup(ctx context.Context, code string) (active bool, found bool)
}

// LayeredMembership combines an immutable probabilistic base with optional
// exact delta and confirmation layers.
//
// Lookup order:
//  1. Delta override (supports additions and revocations).
//  2. Base definite absence.
//  3. Optional exact confirmation of a possible base match.
//  4. Possible match accepted when no exact confirmer is configured.
type LayeredMembership struct {
	base      Membership
	delta     DeltaStore
	confirmer ExactMembership
}

// NewLayeredMembership composes base, delta, and optional exact confirmation.
func NewLayeredMembership(base Membership, delta DeltaStore, confirmer ExactMembership) *LayeredMembership {
	return &LayeredMembership{base: base, delta: delta, confirmer: confirmer}
}

// PossiblyContains applies base+delta lookup and optional exact confirmation.
func (i *LayeredMembership) PossiblyContains(ctx context.Context, code string) bool {
	if i.delta != nil {
		if active, found := i.delta.Lookup(ctx, code); found {
			return active
		}
	}
	if !i.base.PossiblyContains(ctx, code) {
		return false
	}
	if i.confirmer != nil {
		return i.confirmer.Contains(ctx, code)
	}
	return true
}

// MapDelta is a small immutable exact delta suitable for local deployments
// and tests. Production can replace it with Redis or a database adapter.
type MapDelta map[string]bool

// Lookup returns an exact active/revoked override.
func (d MapDelta) Lookup(_ context.Context, code string) (bool, bool) {
	active, found := d[code]
	return active, found
}

// ExactSet is a small immutable exact membership source.
type ExactSet map[string]struct{}

// Contains reports exact membership.
func (s ExactSet) Contains(_ context.Context, code string) bool {
	_, found := s[code]
	return found
}
