package cmd

import "testing"

func TestAthenaQueryBuilder(t *testing.T) {
	a := NewAthenaQueryBuilder()

	t.Log("test ls")
	if a.ls("") != "SHOW DATABASES" {
		t.Fail()
	}

	if a.ls("test_db") != "SHOW TABLES IN test_db" {
		t.Fail()
	}
}
