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

package virtualtable

import (
	"encoding/json"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
)

func createMappingFromEvent(incomingBody *string, indexName *string) (string, error) {
	jsonSource := make(map[string]interface{})
	decoder := json.NewDecoder(strings.NewReader(*incomingBody))
	decoder.UseNumber()
	err := decoder.Decode(&jsonSource)
	if err != nil {
		log.Errorf("createMappingFromEvent: failed to decode json incoming event body=%v, err=%v", *incomingBody, err)
		return "", err
	}
	indexToMapping := make(map[string]interface{})
	m := make(map[string]interface{})
	flat_json := utils.Flatten(jsonSource)
	for key, val := range flat_json {

		if val == nil {
			continue
		}

		switch val := val.(type) {
		case string:
			if val == config.GetTimeStampKey() {
				m[key] = map[string]interface{}{
					"type": "date"}
			} else {
				m[key] = map[string]interface{}{
					"type": "string"}
			}

		case json.Number:
			m[key] = map[string]interface{}{
				"type": "number"}

		case bool:
			m[key] = map[string]interface{}{
				"type": "bool"}

		default:
			log.Errorf("createMappingFromEvent: unknown type=%T. Value=%v, colName=%v", val, val, key)
		}
	}
	indexToMapping[*indexName] = map[string]interface{}{
		"mappings": m}
	JsonBody, err := json.Marshal(indexToMapping)
	if err != nil {
		log.Errorf("createMappingFromEvent: cannot Marshal the data, data=%v, err=%v", indexToMapping, err)
		return "", err
	}
	return string(JsonBody), nil
}

func AddMappingFromADoc(indexName *string, incomingBody *string, orgid uint64) error {
	jsonBody, err := createMappingFromEvent(incomingBody, indexName)
	if err != nil {
		log.Errorf("AddMappingFromADoc: cannot create mapping from the event with indexName=%v, incomingBody=%v, err=%v", indexName, incomingBody, err)
		return err
	}
	return AddMapping(indexName, &jsonBody, orgid)
}
