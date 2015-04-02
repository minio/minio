package mxj

var xmldata = []byte(`
<book>
	<author>William H. Gaddis</author>
	<title>The Recognitions</title>
	<review>One of the seminal American novels of the 20th century.</review>
</book>
<book>
	<author>William H. Gaddis</author>
	<title>JR</title>
	<review>Won the National Book Award.</end_tag_error>
</book>
<book>
	<author>Austin Tappan Wright</author>
	<title>Islandia</title>
	<review>An example of earlier 20th century American utopian fiction.</review>
</book>
<book>
	<author>John Hawkes</author>
	<title>The Beetle Leg</title>
	<review>A lyrical novel about the construction of Ft. Peck Dam in Montana.</review>
</book>
<book>
	<author>
		<first_name>T.E.</first_name>
		<last_name>Porter</last_name>
	</author>
	<title>King's Day</title>
	<review>A magical novella.</review>
</book>`)

var jsondata = []byte(`
 {"book":{"author":"William H. Gaddis","review":"One of the great seminal American novels of the 20th century.","title":"The Recognitions"}}
{"book":{"author":"Austin Tappan Wright","review":"An example of earlier 20th century American utopian fiction.","title":"Islandia"}}
{"book":{"author":"John Hawkes","review":"A lyrical novel about the construction of Ft. Peck Dam in Montana.","title":"The Beetle Leg"}}
{"book":{"author":{"first_name":"T.E.","last_name":"Porter"},"review":"A magical novella.","title":"King's Day"}}
{ "here":"we", "put":"in", "an":error }`)
