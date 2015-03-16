package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Date - [0000-00-00]
type Date struct {
	Year  int16
	Month byte
	Day   byte
}

// String output in yyyy-mm-dd format
func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

// IsZero true if date is 0000-00-00
func (d Date) IsZero() bool {
	return d.Day == 0 && d.Month == 0 && d.Year == 0
}

// Convert string date in format YYYY-MM-DD to Date.
// Leading and trailing spaces are ignored. If format is invalid returns zero.
func parseDate(str string) (d Date, err error) {
	str = strings.TrimSpace(str)
	if str == "0000-00-00" {
		return
	}
	var (
		y, m, n int
	)
	if len(str) != 10 || str[4] != '-' || str[7] != '-' {
		err = errors.New("Invalid 0000-00-000 style DATE string: " + str)
		return
	}
	if y, err = strconv.Atoi(str[0:4]); err != nil {
		return
	}
	if m, err = strconv.Atoi(str[5:7]); err != nil {
		return
	}
	if m < 1 || m > 12 {
		err = errors.New("Invalid 0000-00-000 style DATE string: " + str)
		return
	}
	if n, err = strconv.Atoi(str[8:10]); err != nil {
		return
	}
	if n < 1 || n > 31 {
		err = errors.New("Invalid 0000-00-000 style DATE string: " + str)
		return
	}
	d.Year = int16(y)
	d.Month = byte(m)
	d.Day = byte(n)
	return
}
