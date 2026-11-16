// Package hashtable implements the key-value table behind the store: FNV-1a
// hashing, separate chaining, growth and shrinking, and bounded scans.
// It knows nothing about HTTP, time or expiry rules, and it is not safe for
// concurrent use; the store serializes access.
package hashtable
