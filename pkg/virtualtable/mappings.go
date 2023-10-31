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

package virtualtable

import (
	"encoding/json"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
)

func createMappingFromEvent(incomingBody *string, indexName *string) (error, string) {
	jsonSource := make(map[string]interface{})
	decoder := json.NewDecoder(strings.NewReader(*incomingBody))
	decoder.UseNumber()
	err := decoder.Decode(&jsonSource)
	indexToMapping := make(map[string]interface{})
	m := make(map[string]interface{})
	if err != nil {
		log.Errorf("createMappingFromEvent: failed to decode json event, err=%v", err)
		return err, ""
	}
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
			log.Errorf("createMappingFromEvent: dont know how to convert type=%T for colName=%v", val, key)
		}
	}
	indexToMapping[*indexName] = map[string]interface{}{
		"mappings": m}
	JsonBody, err := json.Marshal(indexToMapping)
	if err != nil {
		log.Errorf("createMappingFromEvent: cannot Marshal the data, err=%v", err)
		return err, ""
	}
	return nil, string(JsonBody)
}

func AddMappingFromADoc(indexName *string, incomingBody *string, orgid uint64) error {
	err, jsonBody := createMappingFromEvent(incomingBody, indexName)
	if err != nil {
		log.Errorf("AddMappingFromADoc: cannot create mapping from the event with indexName=%v, err=%v", indexName, err)
		return err
	}
	return AddMapping(indexName, &jsonBody, orgid)
}
