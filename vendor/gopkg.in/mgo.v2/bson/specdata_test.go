package bson_test

var specTests = []string{
	`
--- 
description: "Array type"
documents:
  - 
    decoded: 
      a : []
    encoded: 0D000000046100050000000000 
  - 
    decoded: 
      a: [10]
    encoded: 140000000461000C0000001030000A0000000000
  -
    # Decode an array that uses an empty string as the key
    decodeOnly : true
    decoded: 
      a: [10]
    encoded: 130000000461000B00000010000A0000000000
  -
    # Decode an array that uses a non-numeric string as the key
    decodeOnly : true
    decoded: 
      a: [10]
    encoded: 150000000461000D000000106162000A0000000000


`, `
--- 
description: "Boolean type"
documents: 
  - 
    encoded: "090000000862000100"
    decoded: { "b" : true }
  - 
    encoded: "090000000862000000"
    decoded: { "b" : false }
    
 
  `, `
--- 
description: "Corrupted BSON"
documents:
  -
    encoded: "09000000016600"
    error: "truncated double"
  -
    encoded: "09000000026600"
    error: "truncated string"
  -
    encoded: "09000000036600"
    error: "truncated document"
  -
    encoded: "09000000046600"
    error: "truncated array"
  -
    encoded: "09000000056600"
    error: "truncated binary"
  -
    encoded: "09000000076600"
    error: "truncated objectid"
  -
    encoded: "09000000086600"
    error: "truncated boolean"
  -
    encoded: "09000000096600"
    error: "truncated date"
  -
    encoded: "090000000b6600"
    error: "truncated regex"
  -
    encoded: "090000000c6600"
    error: "truncated db pointer"
  -
    encoded: "0C0000000d6600"
    error: "truncated javascript"
  -
    encoded: "0C0000000e6600"
    error: "truncated symbol"
  -
    encoded: "0C0000000f6600"
    error: "truncated javascript with scope"
  -
    encoded: "0C000000106600"
    error: "truncated int32"
  -
    encoded: "0C000000116600"
    error: "truncated timestamp"
  -
    encoded: "0C000000126600"
    error: "truncated int64"
  - 
    encoded: "0400000000"
    error: basic
  - 
    encoded: "0500000001"
    error: basic
  - 
    encoded: "05000000"
    error: basic
  - 
    encoded: "0700000002610078563412"
    error: basic
  - 
    encoded: "090000001061000500"
    error: basic
  - 
    encoded: "00000000000000000000"
    error: basic
  - 
    encoded: "1300000002666f6f00040000006261720000"
    error: "basic"
  - 
    encoded: "1800000003666f6f000f0000001062617200ffffff7f0000"
    error: basic
  - 
    encoded: "1500000003666f6f000c0000000862617200010000"
    error: basic
  - 
    encoded: "1c00000003666f6f001200000002626172000500000062617a000000"
    error: basic
  - 
    encoded: "1000000002610004000000616263ff00"
    error: string is not null-terminated
  - 
    encoded: "0c0000000200000000000000"
    error: bad_string_length
  - 
    encoded: "120000000200ffffffff666f6f6261720000"
    error: bad_string_length
  - 
    encoded: "0c0000000e00000000000000"
    error: bad_string_length
  - 
    encoded: "120000000e00ffffffff666f6f6261720000"
    error: bad_string_length
  - 
    encoded: "180000000c00fa5bd841d6585d9900"
    error: ""
  - 
    encoded: "1e0000000c00ffffffff666f6f626172005259b56afa5bd841d6585d9900"
    error: bad_string_length
  - 
    encoded: "0c0000000d00000000000000"
    error: bad_string_length
  - 
    encoded: "0c0000000d00ffffffff0000"
    error: bad_string_length
  - 
    encoded: "1c0000000f001500000000000000000c000000020001000000000000"
    error: bad_string_length
  - 
    encoded: "1c0000000f0015000000ffffffff000c000000020001000000000000"
    error: bad_string_length
  - 
    encoded: "1c0000000f001500000001000000000c000000020000000000000000"
    error: bad_string_length
  - 
    encoded: "1c0000000f001500000001000000000c0000000200ffffffff000000"
    error: bad_string_length
  - 
    encoded: "0E00000008616263646566676869707172737475"
    error: "Run-on CString"
  - 
    encoded: "0100000000"
    error: "An object size that's too small to even include the object size, but is correctly encoded, along with a correct EOO (and no data)"
  - 
    encoded: "1a0000000e74657374000c00000068656c6c6f20776f726c6400000500000000"
    error: "One object, but with object size listed smaller than it is in the data"
  - 
    encoded: "05000000"
    error: "One object, missing the EOO at the end"
  - 
    encoded: "0500000001"
    error: "One object, sized correctly, with a spot for an EOO, but the EOO is 0x01"
  - 
    encoded: "05000000ff"
    error: "One object, sized correctly, with a spot for an EOO, but the EOO is 0xff"
  - 
    encoded: "0500000070"
    error: "One object, sized correctly, with a spot for an EOO, but the EOO is 0x70"
  - 
    encoded: "07000000000000"
    error: "Invalid BSON type low range"
  - 
    encoded: "07000000800000"
    error: "Invalid BSON type high range"
  -
    encoded: "090000000862000200"
    error: "Invalid boolean value of 2"
  - 
    encoded: "09000000086200ff00"
    error: "Invalid boolean value of -1"
  `, `
--- 
description: "Int32 type"
documents: 
  - 
    decoded: 
      i: -2147483648
    encoded: 0C0000001069000000008000
  - 
    decoded: 
      i: 2147483647
    encoded: 0C000000106900FFFFFF7F00
  - 
    decoded: 
      i: -1
    encoded: 0C000000106900FFFFFFFF00
  - 
    decoded: 
      i: 0
    encoded: 0C0000001069000000000000
  - 
    decoded: 
      i: 1
    encoded: 0C0000001069000100000000

`, `
--- 
description: "String type"
documents:
  - 
    decoded: 
      s : ""
    encoded: 0D000000027300010000000000
  - 
    decoded: 
      s: "a"
    encoded: 0E00000002730002000000610000
  - 
    decoded: 
      s: "This is a string"
    encoded: 1D0000000273001100000054686973206973206120737472696E670000
  - 
    decoded: 
      s: "κόσμε"
    encoded: 180000000273000C000000CEBAE1BDB9CF83CEBCCEB50000
`}
