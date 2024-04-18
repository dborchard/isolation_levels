package ssi

import "time"

type DataVersion struct {
	// only applicable for ssi
	VersionID int
	CreatedBy int

	// only for percolator
	Lock  int64 // Start timestamp of the transaction that has locked this row
	Write int64 // Commit timestamp of the transaction that last wrote to this row

	Data      string
	CreatedAt time.Time
}
