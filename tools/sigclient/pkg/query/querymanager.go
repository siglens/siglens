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

package query

type queryTemplate struct {
	validator        queryValidator
	timeRangeSeconds uint64
	maxInProgress    int
}

type queryManager struct {
	templates         map[*queryTemplate]int // Maps to number in progress
	inProgressQueries []queryValidator
	runnableQueries   []queryValidator
}

func NewQueryManager(templates []*queryTemplate) *queryManager {
	templatesMap := make(map[*queryTemplate]int)
	for _, template := range templates {
		templatesMap[template] = 0
	}

	return &queryManager{
		templates:         templatesMap,
		inProgressQueries: make([]queryValidator, 0),
		runnableQueries:   make([]queryValidator, 0),
	}
}

func (qm *queryManager) HandleIngestedLogs(logs []map[string]interface{}) {
	qm.addInProgessQueries()
	qm.sendToValidators(logs)
	qm.moveToRunnable()

	if qm.canRunMore() {
		qm.startQuery()
	}
}

func (qm *queryManager) addInProgessQueries() {
	panic("not implemented")
}

func (qm *queryManager) sendToValidators(logs []map[string]interface{}) {
	panic("not implemented")
}

func (qm *queryManager) moveToRunnable() {
	panic("not implemented")
}

func (qm *queryManager) canRunMore() bool {
	panic("not implemented")
}

func (qm *queryManager) startQuery() {
	panic("not implemented")
}
