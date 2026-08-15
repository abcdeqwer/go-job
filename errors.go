package gojob

import (
	"errors"
	"strconv"
)

// Sentinel errors the engine distinguishes. They matter because conflating them is a
// recurring source of silent misbehaviour: "someone else is running it" and "the row is
// missing" produce the same zero-row result from a guarded UPDATE, and treating the second
// as the first turns a broken installation into a silently idle one.
var (
	// ErrContended means another holder legitimately owns the job right now. Ordinary, and
	// the only zero-row outcome that is not a fault.
	ErrContended = errors.New("gojob: job is held by another execution")

	// ErrFenced means this attempt's token or epoch is no longer current. Every guarded
	// write returns it rather than failing silently, so a revived process learns it has
	// been replaced instead of continuing to write.
	ErrFenced = errors.New("gojob: attempt is fenced")

	// ErrMissingState means the job state row does not exist. Fails closed: it is a broken
	// installation, never contention.
	ErrMissingState = errors.New("gojob: job state row is missing")

	// ErrSchemaIdentity means a coordination schema does not present the tenant and uuid
	// the registry expects — a DSN pointing at another tenant's schema, an empty one, or a
	// restored snapshot.
	ErrSchemaIdentity = errors.New("gojob: coordination schema identity mismatch")

	// ErrSchemaVersion means the schema is not the version this build requires.
	ErrSchemaVersion = errors.New("gojob: coordination schema version mismatch")

	// ErrTimeZone means a tenant's database session time zone differs from the configured
	// business Location. Caught at admission rather than surfacing as an eight-hour
	// scheduling error at 2am.
	ErrTimeZone = errors.New("gojob: database session time zone differs from business location")

	// ErrControlStale means this instance has not read the registry within the staleness
	// limit. It stops claiming, materializing and renewing — so a partitioned instance
	// fences itself rather than remaining an invisible owner while a DSN cutover proceeds.
	ErrControlStale = errors.New("gojob: control database is stale; this instance has self-fenced")
)

func parseInt(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
