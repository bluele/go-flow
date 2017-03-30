package flow

import (
	"fmt"
	"testing"
)

func serialize(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return append(t, '\n'), nil
	case string:
		return []byte(t + "\n"), nil
	}
	return nil, fmt.Errorf("Unknown type: %T", v)
}

func deserialize(b []byte) (interface{}, error) {
	if b[len(b)-1] == '\n' {
		return string(b[:len(b)-1]), nil
	}
	return string(b), nil
}

func TestFileStreaming(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			t.Fatal(e)
		}
	}()

	st, err := NewFileStreaming("./flow_fst.txt", serialize, deserialize)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Destroy()
	if err = st.Write("test1"); err != nil {
		t.Error(err)
	}
	if err = st.Write("test2"); err != nil {
		t.Error(err)
	}
	v, err := st.Read()
	if err != nil {
		t.Error(err)
	}
	if v.(string) != "test1" {
		t.Errorf("%v != %v", v, "test1")
	}
}
