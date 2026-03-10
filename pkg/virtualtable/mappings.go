// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

func AddMappingFromADoc(indexName *string, incomingBody *string, orgid int64) error {
	jsonBody, err := createMappingFromEvent(incomingBody, indexName)
	if err != nil {
		log.Errorf("AddMappingFromADoc: cannot create mapping from the event with indexName=%v, incomingBody=%v, err=%v", indexName, incomingBody, err)
		return err
	}
	return AddMapping(indexName, &jsonBody, orgid)
}
