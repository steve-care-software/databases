package references

import (
	"reflect"
	"testing"
)

func TestAdapter_withoutPeers_Success(t *testing.T) {
	reference := NewReferenceForTests(0)
	adapter := NewAdapter([]byte("0")[0])
	content, err := adapter.ToContent(reference)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retReference, err := adapter.ToReference(content)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if !reflect.DeepEqual(reference, retReference) {
		t.Errorf("the returned reference is invalid")
		return
	}
}
