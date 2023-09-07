package csv

import (
	 "testing"
	 "github.com/minio/minio/internal/s3select/sql"
	 "bytes"
	 "io/ioutil"
	 "strings"
	 "io"
	 "os"
	 "secsys/gout-transformation/pkg/transstruct"
)

func FuzzTestRead(XVl []byte) int {
	t := &testing.T{}
	_ = t
	var skippingTableDriven bool
	_, skippingTableDriven = os.LookupEnv("SKIPPING_TABLE_DRIVEN")
	_ = skippingTableDriven
	transstruct.SetFuzzData(XVl)
	FDG_FuzzGlobal()

	cases := []struct {
		content		string
		recordDelimiter	string
		fieldDelimiter	string
	}{
		{transstruct.GetString("1,2,3\na,b,c\n"), "\n", ","},
		{transstruct.GetString("1,2,3\ta,b,c\t"), "\t", ","},
		{transstruct.GetString("1,2,3\r\na,b,c\r\n"), "\r\n", ","},
	}

	for i, c := range cases {
		var err error
		var record sql.Record
		var result bytes.Buffer

		r, _ := NewReader(ioutil.NopCloser(strings.NewReader(c.content)), &ReaderArgs{
			FileHeaderInfo:			none,
			RecordDelimiter:		c.recordDelimiter,
			FieldDelimiter:			c.fieldDelimiter,
			QuoteCharacter:			defaultQuoteCharacter,
			QuoteEscapeCharacter:		defaultQuoteEscapeCharacter,
			CommentCharacter:		defaultCommentCharacter,
			AllowQuotedRecordDelimiter:	false,
			unmarshaled:			true,
		})

		for {
			record, err = r.Read(record)
			if err != nil {
				break
			}
			opts := sql.WriteCSVOpts{
				FieldDelimiter:	[]rune(c.fieldDelimiter)[0],
				Quote:		'"',
				QuoteEscape:	'"',
				AlwaysQuote:	false,
			}
			record.WriteCSV(&result, opts)
			result.Truncate(result.Len() - 1)
			result.WriteString(c.recordDelimiter)
		}
		r.Close()
		if err != io.EOF {
			t.Fatalf("Case %d failed with %s", i, err)
		}

		if result.String() != c.content {
			t.Errorf("Case %d failed: expected %v result %v", i, c.content, result.String())
		}
	}

	return 1
}

func FDG_FuzzGlobal() {

}
