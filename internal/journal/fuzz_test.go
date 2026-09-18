package journal

import "testing"

func FuzzWALDecode(f *testing.F) {
	rec := sampleRecord()
	encoded, err := EncodeRecord(rec)
	if err != nil {
		f.Fatalf("encode seed: %v", err)
	}
	f.Add(encoded)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeRecord(data)
	})
}
