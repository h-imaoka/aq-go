package cmd

import (
	"reflect"
	"testing"
)

func TestSchema(t *testing.T) {
	var schema *Schema
	var expected map[string]string
	var err error

	t.Log("test converting column_type")

	t.Log("test converting string")
	schema = newSchema()
	err = schema.appendColumn("str0","string","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "str0", "athenaType": "STRING", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting required")
	schema = newSchema()
	err = schema.appendColumn("str1","string","required")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "str1", "athenaType": "STRING", "nullable": "false"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting nullable")
	schema = newSchema()
	err = schema.appendColumn("str2","string","nullable")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "str2", "athenaType": "STRING", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting integer")
	schema = newSchema()
	err = schema.appendColumn("num","integer","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "num", "athenaType": "BIGINT", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting float")
	schema = newSchema()
	err = schema.appendColumn("f","float","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "f", "athenaType": "DOUBLE", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting boolean")
	schema = newSchema()
	err = schema.appendColumn("bool","boolean","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "bool", "athenaType": "BOOLEAN", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting timestamp")
	schema = newSchema()
	err = schema.appendColumn("ts","timestamp","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "ts", "athenaType": "TIMESTAMP", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting date")
	schema = newSchema()
	err = schema.appendColumn("d","date","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "d", "athenaType": "DATE", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting time")
	schema = newSchema()
	err = schema.appendColumn("t","time","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "t", "athenaType": "TIMESTAMP", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test converting datetime")
	schema = newSchema()
	err = schema.appendColumn("dt", "datetime","")
	if err != nil {
		t.Fail()
	}
	expected = map[string]string{"name": "dt", "athenaType": "TIMESTAMP", "nullable": "true"}
	if !reflect.DeepEqual(schema.Fields[0].toMap(), expected) {
		t.Fail()
	}

	t.Log("test not supporting bytes")
	schema = newSchema()
	err = schema.appendColumn("ng", "bytes","")
	if err == nil {
		t.Fail()
	}

	t.Log("test not implemented record")
	schema = newSchema()
	err = schema.appendColumn("ng", "record","")
	if err == nil {
		t.Fail()
	}
}

func TestSchemaLoader(t *testing.T) {
	var s *Schema
	var err error

	sl := NewSchemaLoader()
	expected := "`str0` STRING,`str1` STRING,`str2` STRING"

	t.Log("load from file")
	s, err = sl.load("../resource/schema.json")
	if err != nil {
		t.Fail()
	}
	if s.toString() != expected {
		t.Fail()
	}

	t.Log("load from string")
	s, err = sl.load("str0:STRING,str1:STRING,str2:STRING")
	if err != nil {
		t.Fail()
	}
	if s.toString() != expected {
		t.Fail()
	}
}
