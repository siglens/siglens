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

package query

import (
	"math"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	utils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var TS_FOUR_MONTHS uint64 = 5259486000

func CreateSingleDocReqASTNode(columnName string, columnValue string, isKibana bool, qid uint64) *ASTNode {

	colValEnc, err := CreateDtypeEnclosure(columnValue, qid)
	if err != nil {
		// TODO: handle error
		log.Errorf("qid=%d, CreateSingleDocReqASTNode: failed to create DtypeEnclosure: %v", qid, err)
		return nil
	}
	docFilterCriteria := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: columnName}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: colValEnc}}},
		},
	}

	var startMs, endMs uint64

	if isKibana {
		endMs = math.MaxUint64
		startMs = uint64(0)
	} else {
		endMs = utils.GetCurrentTimeInMs()
		// TODO: when "retention config" is done, pull value from config.
		// StartEpochMs set to 4 months ago
		startMs = endMs - TS_FOUR_MONTHS

	}

	singleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&docFilterCriteria}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: startMs,
			EndEpochMs:   endMs,
		},
	}
	return singleNode
}

func CreateMgetReqASTNode(idColName string, idVal string, docTypeColName string, docTypeVal string, qid uint64) *ASTNode {
	andFilterConditions := make([]*FilterCriteria, 0)
	idDtype, err := CreateDtypeEnclosure(idVal, qid)
	if err != nil {
		log.Errorf("qid=%d, CreateMgetReqASTNode: failed to create DtypeEnclosure error: %+v", qid, err)
	}
	andFilterConditions = append(andFilterConditions,
		&FilterCriteria{
			ExpressionFilter: &ExpressionFilter{
				LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: idColName}}},
				FilterOperator: Equals,
				RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: idDtype}}},
			},
		})

	docTypeValDtype, err := CreateDtypeEnclosure(docTypeVal, qid)
	if err != nil {
		log.Errorf("qid=%d, CreateMgetReqASTNode: failed to create DtypeEnclosure error: %+v", qid, err)
	}
	andFilterConditions = append(andFilterConditions,
		&FilterCriteria{
			ExpressionFilter: &ExpressionFilter{
				LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: docTypeColName}}},
				FilterOperator: Equals,
				RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: docTypeValDtype}}},
			},
		})

	tsNow := utils.GetCurrentTimeInMs()

	singleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: andFilterConditions},
		TimeRange: &dtu.TimeRange{
			// TODO: when "retention config" is done, pull value from config.
			// StartEpochMs set to 4 months ago
			StartEpochMs: tsNow - TS_FOUR_MONTHS,
			//TODO: add a way to deal with this being empty
			EndEpochMs: tsNow,
		},
	}
	return singleNode
}
