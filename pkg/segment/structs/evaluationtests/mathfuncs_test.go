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
	ExpectedAnswer float64
}

func parseSPL(t *testing.T, query string) (*structs.ASTNode, *structs.QueryAggregators) {
	astNode, aggs, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.NoError(t, err)
	return astNode, aggs
}

func evaluateNumericExpr(t *testing.T, aggs *structs.QueryAggregators) float64 {
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	res, err := aggs.EvalExpr.ValueExpr.NumericExpr.Evaluate(fieldToValue)
	assert.NoError(t, err)
	return res
}

func Test_Sigfig(t *testing.T) {
	query := "* | eval col=sigfig(%v)"

	testCases := getTestCasesSigfig()

	for _, test := range testCases {
		_, aggs := parseSPL(t, fmt.Sprintf(query, test.EquationString))
		res := evaluateNumericExpr(t, aggs)
		assert.Equal(t, test.ExpectedAnswer, res)
	}
}

func getTestCasesSigfig() []TestCase {
	return []TestCase{
		// addition/substraction
		{
			EquationString: "2.34 + 1.2",
			ExpectedAnswer: 3.5,
		},
		{
			EquationString: "5.00 + 1.234",
			ExpectedAnswer: 6.23,
		},
		{
			EquationString: "123 + 4.56",
			ExpectedAnswer: 128.0,
		},
		{
			EquationString: "0.1234 - 0.005",
			ExpectedAnswer: 0.118,
		},
		{
			EquationString: "3.1 + 0.004 + 12.55",
			ExpectedAnswer: 15.7,
		},
		// multiplication/division
		{
			EquationString: "2.5 * 1.23",
			ExpectedAnswer: 3.1,
		},
		{
			EquationString: "0.0045 * 12.3",
			ExpectedAnswer: 0.055,
		},
		{
			EquationString: "3.00 * 1.2",
			ExpectedAnswer: 3.6,
		},
		{
			EquationString: "0.078 / 1.23",
			ExpectedAnswer: 0.063,
		},
		// mix - also checks premature rounding
		{
			EquationString: "(2.3 + 0.12) * 1.5",
			ExpectedAnswer: 3.6,
		},
		{
			EquationString: "(1.23 * 0.5) + 3.456",
			ExpectedAnswer: 4.1,
		},
		{
			EquationString: "(0.0012 + 0.034) / 5.6",
			ExpectedAnswer: 0.0063,
		},
		{
			EquationString: "((4.50 - 2.1) * 3.0) + 0.005",
			ExpectedAnswer: 7.2,
		},
		{
			EquationString: "(5.123 - 5.120) * 123.45",
			ExpectedAnswer: 0.4,
		},
		{
			EquationString: "1200 / 3.0",
			ExpectedAnswer: 400.0,
		},
		{
			EquationString: "12345.67 + 0.000001",
			ExpectedAnswer: 12345.67,
		},
		{
			EquationString: "2.0 + 3.0 * 4.0",
			ExpectedAnswer: 14.0,
		},
		{
			EquationString: "2.0 + 3.0 + 4.0",
			ExpectedAnswer: 9.0,
		},
		{
			EquationString: "2.0 + 4.0 + 3.0",
			ExpectedAnswer: 9.0,
		},
		{
			EquationString: "(10.0 + 0.11) * 3.0",
			ExpectedAnswer: 30.0,
		},
		{
			EquationString: "(5.123 - 5.120)",
			ExpectedAnswer: 0.003,
		},
		{
			EquationString: "(5.123 - 5.120) * 123.45",
			ExpectedAnswer: 0.4,
		},
		// specific check for premature rounding
		{
			EquationString: "(5.00 / 1.235) + 3.000 + (6.35 / 4.0)",
			ExpectedAnswer: 8.6, // 4.04858… →4.05; 1.5875→1.59; 4.05+3.000+1.59=8.640 → least decimal places=1 → 8.6
		},
		//	1.234+2.2=3.434 →3.4 (1 dp), then 3.4+3.23=6.63→6.6 (1 dp)
		{
			EquationString: "(1.234 + 2.2) + 3.23",
			ExpectedAnswer: 6.7, // still 6.664→6.7 at the very end
		},
	}

}
