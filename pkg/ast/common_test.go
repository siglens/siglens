// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package ast

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_GetDefaultTimechartSpanOptions(t *testing.T) {
	type args struct {
		startEpoch uint64
		endEpoch   uint64
		qid        uint64
	}
	tests := []struct {
		name    string
		args    args
		want    *structs.SpanOptions
		wantErr bool
	}{
		{"startEpoch = 0 should be error", args{0, 1, 1}, nil, true},
		{"endEpoch = 0 should be error", args{1, 0, 1}, nil, true},
		{"<15*60*1000 should be TMSecond with Num = 10",
			args{1, 5*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 10, TimeScalr: utils.TMSecond}, DefaultSettings: false},
			false},
		{"15*60*1000 should be TMSecond with Num = 10",
			args{1, 15*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 10, TimeScalr: utils.TMSecond}, DefaultSettings: false},
			false},
		{"<60*60*1000 should be TMMinute with Num = 1",
			args{1, 30*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMMinute}, DefaultSettings: false},
			false},
		{"60*60*1000 should be TMMinute with Num = 1",
			args{1, 60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMMinute}, DefaultSettings: false},
			false},
		{"<4*60*60*1000 should be TMMinute with Num = 5",
			args{1, 2*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 5, TimeScalr: utils.TMMinute}, DefaultSettings: false},
			false},
		{"4*60*60*1000 should be TMMinute with Num = 5",
			args{1, 4*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 5, TimeScalr: utils.TMMinute}, DefaultSettings: false},
			false},
		{"<24*60*60*1000 should be TMMinute with Num = 30",
			args{1, 20*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 30, TimeScalr: utils.TMMinute}, DefaultSettings: false},
			false},
		{"24*60*60*1000 should be TMMinute with Num = 30",
			args{1, 24*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 30, TimeScalr: utils.TMMinute}, DefaultSettings: false},
			false},
		{"<7*24*60*60*1000 should be TMHour with Num = 1",
			args{1, 6*24*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMHour}, DefaultSettings: false},
			false},
		{"7*24*60*60*1000 should be TMHour with Num = 1",
			args{1, 7*24*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMHour}, DefaultSettings: false},
			false},
		{"<180*24*60*60*1000 should be TMDay with Num = 1",
			args{1, 179*24*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMDay}, DefaultSettings: false},
			false},
		{"180*24*60*60*1000 should be TMDay with Num = 1",
			args{1, 180*24*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMDay}, DefaultSettings: false},
			false},
		{">180*24*60*60*1000 should be TMDay with Num = 1",
			args{1, 181*24*60*60*1000 + 1, 1},
			&structs.SpanOptions{SpanLength: &structs.SpanLength{Num: 1, TimeScalr: utils.TMMonth}, DefaultSettings: false},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetDefaultTimechartSpanOptions(tt.args.startEpoch, tt.args.endEpoch, tt.args.qid)
			assert.Equal(t, err != nil, tt.wantErr)
			assert.Equal(t, got, tt.want)
		})
	}
}
