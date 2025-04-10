// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package event

import (
	"encoding/xml"
	"reflect"
	"strings"
	"testing"
)

func TestValidateFilterRuleValue(t *testing.T) {
	testCases := []struct {
		value     string
		expectErr bool
	}{
		{"foo/.", true},
		{"../foo", true},
		{`foo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/bazfoo/bar/baz`, true},
		{string([]byte{0xff, 0xfe, 0xfd}), true},
		{`foo\bar`, true},
		{"Hello/世界", false},
	}

	for i, testCase := range testCases {
		err := ValidateFilterRuleValue(testCase.value)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestFilterRuleUnmarshalXML(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult *FilterRule
		expectErr      bool
	}{
		{[]byte(`<FilterRule></FilterRule>`), nil, true},
		{[]byte(`<FilterRule><Name></Name></FilterRule>`), nil, true},
		{[]byte(`<FilterRule><Value></Value></FilterRule>`), nil, true},
		{[]byte(`<FilterRule><Name></Name><Value></Value></FilterRule>`), nil, true},
		{[]byte(`<FilterRule><Name>Prefix</Name><Value>Hello/世界</Value></FilterRule>`), nil, true},
		{[]byte(`<FilterRule><Name>ends</Name><Value>foo/bar</Value></FilterRule>`), nil, true},
		{[]byte(`<FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule>`), &FilterRule{"prefix", "Hello/世界"}, false},
		{[]byte(`<FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule>`), &FilterRule{"suffix", "foo/bar"}, false},
	}

	for i, testCase := range testCases {
		result := &FilterRule{}
		err := xml.Unmarshal(testCase.data, result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestFilterRuleListUnmarshalXML(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult *FilterRuleList
		expectErr      bool
	}{
		{[]byte(`<S3Key><FilterRule><Name>suffix</Name><Value>Hello/世界</Value></FilterRule><FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule></S3Key>`), nil, true},
		{[]byte(`<S3Key><FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule><FilterRule><Name>prefix</Name><Value>foo/bar</Value></FilterRule></S3Key>`), nil, true},
		{[]byte(`<S3Key><FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule></S3Key>`), &FilterRuleList{[]FilterRule{{"prefix", "Hello/世界"}}}, false},
		{[]byte(`<S3Key><FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule></S3Key>`), &FilterRuleList{[]FilterRule{{"suffix", "foo/bar"}}}, false},
		{[]byte(`<S3Key><FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule><FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule></S3Key>`), &FilterRuleList{[]FilterRule{{"prefix", "Hello/世界"}, {"suffix", "foo/bar"}}}, false},
	}

	for i, testCase := range testCases {
		result := &FilterRuleList{}
		err := xml.Unmarshal(testCase.data, result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestFilterRuleListPattern(t *testing.T) {
	testCases := []struct {
		filterRuleList FilterRuleList
		expectedResult string
	}{
		{FilterRuleList{}, ""},
		{FilterRuleList{[]FilterRule{{"prefix", "Hello/世界"}}}, "Hello/世界*"},
		{FilterRuleList{[]FilterRule{{"suffix", "foo/bar"}}}, "*foo/bar"},
		{FilterRuleList{[]FilterRule{{"prefix", "Hello/世界"}, {"suffix", "foo/bar"}}}, "Hello/世界*foo/bar"},
	}

	for i, testCase := range testCases {
		result := testCase.filterRuleList.Pattern()

		if result != testCase.expectedResult {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestQueueUnmarshalXML(t *testing.T) {
	dataCase1 := []byte(`
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>`)

	dataCase2 := []byte(`
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>`)

	dataCase3 := []byte(`
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>`)

	testCases := []struct {
		data      []byte
		expectErr bool
	}{
		{dataCase1, false},
		{dataCase2, false},
		{dataCase3, true},
	}

	for i, testCase := range testCases {
		err := xml.Unmarshal(testCase.data, &Queue{})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestQueueValidate(t *testing.T) {
	data := []byte(`
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>`)
	queue1 := &Queue{}
	if err := xml.Unmarshal(data, queue1); err != nil {
		panic(err)
	}

	data = []byte(`
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>`)
	queue2 := &Queue{}
	if err := xml.Unmarshal(data, queue2); err != nil {
		panic(err)
	}

	data = []byte(`
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:eu-west-2:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>`)
	queue3 := &Queue{}
	if err := xml.Unmarshal(data, queue3); err != nil {
		panic(err)
	}

	targetList1 := NewTargetList(t.Context())

	targetList2 := NewTargetList(t.Context())
	if err := targetList2.Add(&ExampleTarget{TargetID{"1", "webhook"}, false, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		queue      *Queue
		region     string
		targetList *TargetList
		expectErr  bool
	}{
		{queue1, "eu-west-1", nil, true},
		{queue2, "us-east-1", targetList1, true},
		{queue3, "", targetList2, false},
		{queue2, "us-east-1", targetList2, false},
	}

	for i, testCase := range testCases {
		err := testCase.queue.Validate(testCase.region, testCase.targetList)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestQueueSetRegion(t *testing.T) {
	data := []byte(`
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>`)
	queue1 := &Queue{}
	if err := xml.Unmarshal(data, queue1); err != nil {
		panic(err)
	}

	data = []byte(`
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs::1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>`)
	queue2 := &Queue{}
	if err := xml.Unmarshal(data, queue2); err != nil {
		panic(err)
	}

	testCases := []struct {
		queue          *Queue
		region         string
		expectedResult ARN
	}{
		{queue1, "eu-west-1", ARN{TargetID{"1", "webhook"}, "eu-west-1"}},
		{queue1, "", ARN{TargetID{"1", "webhook"}, ""}},
		{queue2, "us-east-1", ARN{TargetID{"1", "webhook"}, "us-east-1"}},
		{queue2, "", ARN{TargetID{"1", "webhook"}, ""}},
	}

	for i, testCase := range testCases {
		testCase.queue.SetRegion(testCase.region)
		result := testCase.queue.ARN

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestQueueToRulesMap(t *testing.T) {
	data := []byte(`
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>`)
	queueCase1 := &Queue{}
	if err := xml.Unmarshal(data, queueCase1); err != nil {
		panic(err)
	}

	data = []byte(`
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>`)
	queueCase2 := &Queue{}
	if err := xml.Unmarshal(data, queueCase2); err != nil {
		panic(err)
	}

	rulesMapCase1 := NewRulesMap([]Name{ObjectAccessedAll, ObjectCreatedAll, ObjectRemovedAll}, "*", TargetID{"1", "webhook"})
	rulesMapCase2 := NewRulesMap([]Name{ObjectCreatedPut}, "images/*jpg", TargetID{"1", "webhook"})

	testCases := []struct {
		queue          *Queue
		expectedResult RulesMap
	}{
		{queueCase1, rulesMapCase1},
		{queueCase2, rulesMapCase2},
	}

	for i, testCase := range testCases {
		result := testCase.queue.ToRulesMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestConfigUnmarshalXML(t *testing.T) {
	dataCase1 := []byte(`
<NotificationConfiguration   xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)

	dataCase2 := []byte(`
	<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	   <QueueConfiguration>
	      <Id>1</Id>
	       <Filter>
	           <S3Key>
	               <FilterRule>
	                   <Name>prefix</Name>
	                   <Value>images/</Value>
	               </FilterRule>
	               <FilterRule>
	                   <Name>suffix</Name>
	                   <Value>jpg</Value>
	               </FilterRule>
	           </S3Key>
	      </Filter>
	      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
	      <Event>s3:ObjectCreated:Put</Event>
	   </QueueConfiguration>
	</NotificationConfiguration>
	`)

	dataCase3 := []byte(`
	<NotificationConfiguration>
	   <QueueConfiguration>
	      <Id>1</Id>
	      <Filter></Filter>
	      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
	      <Event>s3:ObjectAccessed:*</Event>
	      <Event>s3:ObjectCreated:*</Event>
	      <Event>s3:ObjectRemoved:*</Event>
	   </QueueConfiguration>
	   <QueueConfiguration>
	      <Id>2</Id>
	       <Filter>
	           <S3Key>
	               <FilterRule>
	                   <Name>prefix</Name>
	                   <Value>images/</Value>
	               </FilterRule>
	               <FilterRule>
	                   <Name>suffix</Name>
	                   <Value>jpg</Value>
	               </FilterRule>
	           </S3Key>
	      </Filter>
	      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
	      <Event>s3:ObjectCreated:Put</Event>
	   </QueueConfiguration>
	</NotificationConfiguration>
	`)

	dataCase4 := []byte(`
	<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	   <QueueConfiguration>
	      <Id>1</Id>
	      <Filter></Filter>
	      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
	      <Event>s3:ObjectAccessed:*</Event>
	      <Event>s3:ObjectCreated:*</Event>
	      <Event>s3:ObjectRemoved:*</Event>
	   </QueueConfiguration>
	   <CloudFunctionConfiguration>
	      <Id>1</Id>
	      <Filter>
	             <S3Key>
	                 <FilterRule>
	                     <Name>suffix</Name>
	                     <Value>.jpg</Value>
	                 </FilterRule>
	             </S3Key>
	      </Filter>
	      <Cloudcode>arn:aws:lambda:us-west-2:444455556666:cloud-function-A</Cloudcode>
	      <Event>s3:ObjectCreated:Put</Event>
	   </CloudFunctionConfiguration>
	   <TopicConfiguration>
	      <Topic>arn:aws:sns:us-west-2:444455556666:sns-notification-one</Topic>
	      <Event>s3:ObjectCreated:*</Event>
	  </TopicConfiguration>
	</NotificationConfiguration>
	`)

	dataCase5 := []byte(`<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/" ></NotificationConfiguration>`)

	testCases := []struct {
		data      []byte
		expectErr bool
	}{
		{dataCase1, false},
		{dataCase2, false},
		{dataCase3, false},
		{dataCase4, true},
		// make sure we don't fail when queue is empty.
		{dataCase5, false},
	}

	for i, testCase := range testCases {
		err := xml.Unmarshal(testCase.data, &Config{})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	data := []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config1 := &Config{}
	if err := xml.Unmarshal(data, config1); err != nil {
		panic(err)
	}

	data = []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config2 := &Config{}
	if err := xml.Unmarshal(data, config2); err != nil {
		panic(err)
	}

	data = []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <QueueConfiguration>
      <Id>2</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config3 := &Config{}
	if err := xml.Unmarshal(data, config3); err != nil {
		panic(err)
	}

	targetList1 := NewTargetList(t.Context())

	targetList2 := NewTargetList(t.Context())
	if err := targetList2.Add(&ExampleTarget{TargetID{"1", "webhook"}, false, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		config     *Config
		region     string
		targetList *TargetList
		expectErr  bool
	}{
		{config1, "eu-west-1", nil, true},
		{config2, "us-east-1", targetList1, true},
		{config3, "", targetList2, false},
		{config2, "us-east-1", targetList2, false},
	}

	for i, testCase := range testCases {
		err := testCase.config.Validate(testCase.region, testCase.targetList)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestConfigSetRegion(t *testing.T) {
	data := []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config1 := &Config{}
	if err := xml.Unmarshal(data, config1); err != nil {
		panic(err)
	}

	data = []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs::1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config2 := &Config{}
	if err := xml.Unmarshal(data, config2); err != nil {
		panic(err)
	}

	data = []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <QueueConfiguration>
      <Id>2</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:2:amqp</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config3 := &Config{}
	if err := xml.Unmarshal(data, config3); err != nil {
		panic(err)
	}

	testCases := []struct {
		config         *Config
		region         string
		expectedResult []ARN
	}{
		{config1, "eu-west-1", []ARN{{TargetID{"1", "webhook"}, "eu-west-1"}}},
		{config1, "", []ARN{{TargetID{"1", "webhook"}, ""}}},
		{config2, "us-east-1", []ARN{{TargetID{"1", "webhook"}, "us-east-1"}}},
		{config2, "", []ARN{{TargetID{"1", "webhook"}, ""}}},
		{config3, "us-east-1", []ARN{{TargetID{"1", "webhook"}, "us-east-1"}, {TargetID{"2", "amqp"}, "us-east-1"}}},
		{config3, "", []ARN{{TargetID{"1", "webhook"}, ""}, {TargetID{"2", "amqp"}, ""}}},
	}

	for i, testCase := range testCases {
		testCase.config.SetRegion(testCase.region)
		result := []ARN{}
		for _, queue := range testCase.config.QueueList {
			result = append(result, queue.ARN)
		}

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestConfigToRulesMap(t *testing.T) {
	data := []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config1 := &Config{}
	if err := xml.Unmarshal(data, config1); err != nil {
		panic(err)
	}

	data = []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs::1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config2 := &Config{}
	if err := xml.Unmarshal(data, config2); err != nil {
		panic(err)
	}

	data = []byte(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <QueueConfiguration>
      <Id>2</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:2:amqp</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)
	config3 := &Config{}
	if err := xml.Unmarshal(data, config3); err != nil {
		panic(err)
	}

	rulesMapCase1 := NewRulesMap([]Name{ObjectAccessedAll, ObjectCreatedAll, ObjectRemovedAll}, "*", TargetID{"1", "webhook"})

	rulesMapCase2 := NewRulesMap([]Name{ObjectCreatedPut}, "images/*jpg", TargetID{"1", "webhook"})

	rulesMapCase3 := NewRulesMap([]Name{ObjectAccessedAll, ObjectCreatedAll, ObjectRemovedAll}, "*", TargetID{"1", "webhook"})
	rulesMapCase3.add([]Name{ObjectCreatedPut}, "images/*jpg", TargetID{"2", "amqp"})

	testCases := []struct {
		config         *Config
		expectedResult RulesMap
	}{
		{config1, rulesMapCase1},
		{config2, rulesMapCase2},
		{config3, rulesMapCase3},
	}

	for i, testCase := range testCases {
		result := testCase.config.ToRulesMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestParseConfig(t *testing.T) {
	reader1 := strings.NewReader(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)

	reader2 := strings.NewReader(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)

	reader3 := strings.NewReader(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <QueueConfiguration>
      <Id>2</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
`)

	reader4 := strings.NewReader(`
<NotificationConfiguration  xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <CloudFunctionConfiguration>
      <Id>1</Id>
      <Filter>
             <S3Key>
                 <FilterRule>
                     <Name>suffix</Name>
                     <Value>.jpg</Value>
                 </FilterRule>
             </S3Key>
      </Filter>
      <Cloudcode>arn:aws:lambda:us-west-2:444455556666:cloud-function-A</Cloudcode>
      <Event>s3:ObjectCreated:Put</Event>
   </CloudFunctionConfiguration>
   <TopicConfiguration>
      <Topic>arn:aws:sns:us-west-2:444455556666:sns-notification-one</Topic>
      <Event>s3:ObjectCreated:*</Event>
  </TopicConfiguration>
</NotificationConfiguration>
`)

	targetList1 := NewTargetList(t.Context())

	targetList2 := NewTargetList(t.Context())
	if err := targetList2.Add(&ExampleTarget{TargetID{"1", "webhook"}, false, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		reader     *strings.Reader
		region     string
		targetList *TargetList
		expectErr  bool
	}{
		{reader1, "eu-west-1", nil, true},
		{reader2, "us-east-1", targetList1, true},
		{reader4, "us-east-1", targetList1, true},
		{reader3, "", targetList2, false},
		{reader2, "us-east-1", targetList2, false},
	}

	for i, testCase := range testCases {
		if _, err := testCase.reader.Seek(0, 0); err != nil {
			panic(err)
		}
		_, err := ParseConfig(testCase.reader, testCase.region, testCase.targetList)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
