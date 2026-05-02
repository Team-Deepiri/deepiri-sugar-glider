package service

import "testing"

func TestCountContiguousAckSpans(t *testing.T) {
	tests := []struct {
		name      string
		entryIDs  []string
		wantSpans int
		wantSaved int
	}{
		{
			name:      "empty",
			entryIDs:  nil,
			wantSpans: 0,
			wantSaved: 0,
		},
		{
			name:      "single",
			entryIDs:  []string{"1710000000000-0"},
			wantSpans: 1,
			wantSaved: 0,
		},
		{
			name: "same millisecond contiguous",
			entryIDs: []string{
				"1710000000000-2",
				"1710000000000-0",
				"1710000000000-1",
			},
			wantSpans: 1,
			wantSaved: 2,
		},
		{
			name: "gapped spans",
			entryIDs: []string{
				"1710000000000-0",
				"1710000000000-1",
				"1710000000000-4",
				"1710000000001-0",
				"1710000000001-1",
			},
			wantSpans: 3,
			wantSaved: 2,
		},
		{
			name: "ignores invalid IDs",
			entryIDs: []string{
				"not-a-stream-id",
				"1710000000000-10",
				"1710000000000-11",
			},
			wantSpans: 1,
			wantSaved: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSpans, gotSaved := countContiguousAckSpans(tt.entryIDs)
			if gotSpans != tt.wantSpans || gotSaved != tt.wantSaved {
				t.Fatalf(
					"countContiguousAckSpans() = (%d, %d), want (%d, %d)",
					gotSpans,
					gotSaved,
					tt.wantSpans,
					tt.wantSaved,
				)
			}
		})
	}
}
