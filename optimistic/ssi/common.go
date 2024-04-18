package ssi

import "time"

type DataVersion struct {
	VersionID int
	Data      string
	CreatedBy int
	CreatedAt time.Time
}
