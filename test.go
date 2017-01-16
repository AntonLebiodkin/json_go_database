package main

import (
	"testing"
)

func TestSetGetChecker(t *testing.T) {
	t.Log("Testing SET GET")
	set(nil, "test", "test_key", "test_value")
	if handleGet(nil, []string{"get", "test", "test_key"}) == "" {
		t.Errorf("failed")
	}
}

func TestDeleteChecker(t *testing.T) {
	t.Log("Testing DELETE")
	set(nil, "test", "test_key", "test_value")
	delete(nil, "test", "test_key")
	if handleGet(nil, []string{"get", "test", "test_key"}) != "" {
		t.Errorf("failed")
	}
}
