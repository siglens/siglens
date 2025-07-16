// Copyright (c) 2021-2025 SigScalr, Inc.
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

package evaluationtests

import (
	"fmt"
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

type TestCase struct {
	EquationString string
	ExpectedAnswer any
}

func parseSPL(t *testing.T, query string) (*structs.ASTNode, *structs.QueryAggregators) {
	astNode, aggs, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.NoError(t, err)
	return astNode, aggs
}

func evaluateTextExpr(t *testing.T, aggs *structs.QueryAggregators) string {
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	res, err := aggs.EvalExpr.ValueExpr.StringExpr.TextExpr.EvaluateText(fieldToValue)
	assert.NoError(t, err)
	return res
}

func Test_Printf(t *testing.T) {
	query := "* | %v"

	testCases := getTestCasesPrintf()

	for _, test := range testCases {
		_, aggs := parseSPL(t, fmt.Sprintf(query, test.EquationString))
		res := evaluateTextExpr(t, aggs)
		assert.Equal(t, test.ExpectedAnswer, res)
	}
}

func getTestCasesPrintf() []TestCase {
	return []TestCase{
		{
			EquationString: `eval result=printf("%+06d", 42)`,
			ExpectedAnswer: `+00042`,
		},
		{
			EquationString: `eval result=printf("%-#8X!", 3735928559)`,
			ExpectedAnswer: `0XDEADBEEF!`,
		},
		{
			EquationString: `eval result=printf("%+#12.4E", 0.0012345)`,
			ExpectedAnswer: ` +1.2345E-03`,
		},
		{
			EquationString: `eval result=printf("%10.3A", 3.1415926535)`,
			ExpectedAnswer: `0X1.922P+01`,
		},
		{
			EquationString: `eval result=printf("%c = %d%%", 128156, 100)`,
			ExpectedAnswer: `💜 = 100%`,
		},
		{
			EquationString: `eval result=printf("%02d-%02d-%d", 5, 7, 2025)`,
			ExpectedAnswer: `05-07-2025`,
		},
		{
			EquationString: `eval result=printf("%#08o", 10)`,
			ExpectedAnswer: `00000012`,
		},
		{
			EquationString: `eval result=printf("%'.5g", 1234567.89)`,
			ExpectedAnswer: `1,234,600`,
		},
		{
			EquationString: `eval result=printf("Value=0x%04X!", 255)`,
			ExpectedAnswer: `Value=0x00FF!`,
		},
		{
			EquationString: `eval result=printf("%'d", 1234567)`,
			ExpectedAnswer: `1,234,567`,
		},
		{
			EquationString: `eval result=printf("%'10d", 987654321)`,
			ExpectedAnswer: ` 987,654,321`,
		},
		{
			EquationString: `eval result=printf("%'15.2f", 1234567.89)`,
			ExpectedAnswer: `     1,234,567.89`,
		},
		{
			EquationString: `eval result=printf("%'+15.2f", 1234567.89)`,
			ExpectedAnswer: `    +1,234,567.89`,
		},
		{
			EquationString: `eval result=printf("%'#15.0f", 1000000.0)`,
			ExpectedAnswer: `       1,000,000`,
		},
		{
			EquationString: `eval result=printf("%'015d", 1234567)`,
			ExpectedAnswer: `000000001,234,567`,
		},
		{
			EquationString: `eval result=printf("%+10.2f", 1234.5)`,
			ExpectedAnswer: `  +1234.50`,
		},
		{
			EquationString: `eval result=printf("% 10.2f", 1234.5)`,
			ExpectedAnswer: `   1234.50`, // space for sign
		},
		{
			EquationString: `eval result=printf("%+'10.2f", 1234.5)`,
			ExpectedAnswer: `  +1,234.50`,
		},
		{
			EquationString: `eval result=printf("%' 10.2f", 1234.5)`,
			ExpectedAnswer: `   1,234.50`,
		},
		{
			EquationString: `eval result=printf("%'+010.2f", 1234.5)`,
			ExpectedAnswer: `+001,234.50`, // leading zero, sign, and grouping
		},
		{
			EquationString: `eval result=printf("%'+ 010.2f", 1234.5)`,
			ExpectedAnswer: `+001,234.50`,
		},
		{
			EquationString: `eval result=printf("%'+15.2f", 9876543.21)`,
			ExpectedAnswer: `    +9,876,543.21`,
		},
		{
			EquationString: `eval result=printf("%' 15.0f", 1000000.0)`,
			ExpectedAnswer: `        1,000,000`,
		},
		{
			EquationString: `eval result=printf("%'+020.2f", 1234.5)`,
			ExpectedAnswer: `+0000000000001,234.50`,
		},
		{
			EquationString: `eval result=printf("%'20.0f", 1234567890.0)`,
			ExpectedAnswer: `          1,234,567,890`,
		},
		{
			EquationString: `eval result=printf("%'d %'+10.2f %g", 123456, 987654.321, 0.0000123)`,
			ExpectedAnswer: "123,456 +987,654.32 1.23e-05",
		},
		{
			EquationString: `eval result=printf("%'+15d %'10.2f %0.1g", 1000000, 987654.32, 3.14159)`,
			ExpectedAnswer: "       +1,000,000  987,654.32 3",
		},
		{
			EquationString: `eval result=printf("%'10d %'+#12.0f %'g", 100000, 1234567.0, 999999.99)`,
			ExpectedAnswer: "    100,000    +1,234,567 999,999.99",
		},
		{
			EquationString: `eval result=printf("%0+10d %'12.3f %'10g", 4567, 123456.789, 7654321.1)`,
			ExpectedAnswer: "+000004567   123,456.789 7,654,321.1",
		},
		{
			EquationString: `eval result=printf("%'+d %'+f %'.2f", 1000000, 12345.67, 9876543.21)`,
			ExpectedAnswer: "+1,000,000 +12,345.670000 9,876,543.21",
		},
		{
			EquationString: `eval result=printf("%'+#10.0f %'15.2f %0+8d", 1000000.0, 12345678.9, 99)`,
			ExpectedAnswer: " +1,000,000     12,345,678.90 +0000099",
		},
		{
			EquationString: `eval result=printf("Rain Percentage: %+12.2f/%*.*f. Sample of: %'d Date: %d-0%d-%d", 92.233433, 3, 1, 100.011111, 10000000000, 12, 1, 25)`,
			ExpectedAnswer: "Rain Percentage:       +92.23/100.0. Sample of: 10,000,000,000 Date: 12-01-25",
		},
		{
			EquationString: `eval result=printf("Rain Percentage: %2.2f%", 92.233433)`,
			ExpectedAnswer: "Rain Percentage: 92.23%",
		},
		{
			EquationString: `eval result=printf("%")`,
			ExpectedAnswer: "%",
		},
	}
}
