// Package store is the core of mini-Redis, usable without HTTP. It owns the
// hash table and adds input validation, locking, TTL expiry, the data limit
// and optional append-only-file persistence with replay, rewrite and repair.
package store
