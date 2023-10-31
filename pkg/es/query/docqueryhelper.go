/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
