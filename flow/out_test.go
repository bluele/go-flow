package flow

import "testing"

func TestFileStreaming(t *testing.T) {
	st, err := NewFileStreaming("/tmp/flow_fst.txt", nil)
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
	if s := String(v); s != "test1" {
		t.Errorf("%v != %v", s, "test1")
	}
}
