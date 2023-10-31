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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_createMappingFromEvent(t *testing.T) {
	var loadDataBytes = []byte(`{"event_id": "f533f3d4-a521-4067-b59b-628bcf8fba62", "timestamp": 1628862769706, "eventType": "pageview", "page_url": "http://www.henry.info/blog/explore/homepage/", "page_url_path": "http://www.johnson.com/", "referer_url": "https://mccall-chavez.com/", "referer_url_scheme": "HEAD", "referer_url_port": 47012, "referer_medium": "bing", "utm_medium": "Beat.", "utm_source": "Edge politics.", "utm_content": "Fly.",
	"utm_campaign": "Development green.", "click_id": "c21ff7e1-2d96-4b21-8415-9b69f882a593", "geo_latitude": "51.42708", "geo_longitude": "-0.91979", "geo_country": "GB", "geo_timezone": "Europe/London", "geo_region_name": "Lower Earley", "ip_address": "198.13.58.1", "browser_name": "chrome", "browser_user_agent": "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_10_4 rv:5.0; iu-CA) AppleWebKit/532.43.2 (KHTML, like Gecko) Version/5.0 Safari/532.43.2",
	"browser_language": "Part.", "os": "Linux", "os_name": "MacOS", "os_timezone": "Europe/Berlin", "device_type": "hardware", "device_is_mobile": true, "user_custom_id": "petersmichaela@hotmail.com", "user_domain_id": "c8aad4b3-0097-430e-8c74-3a2becbd41f9"}
`)
	body := string(loadDataBytes)
	index := "1234"

	_, result := createMappingFromEvent(&body, &index)
	fmt.Println(result)

	expected := "{\"1234\":{\"mappings\":{\"browser_language\":{\"type\":\"string\"},\"browser_name\":{\"type\":\"string\"},\"browser_user_agent\":{\"type\":\"string\"},\"click_id\":{\"type\":\"string\"},\"device_is_mobile\":{\"type\":\"bool\"},\"device_type\":{\"type\":\"string\"},\"eventType\":{\"type\":\"string\"},\"event_id\":{\"type\":\"string\"},\"geo_country\":{\"type\":\"string\"},\"geo_latitude\":{\"type\":\"string\"},\"geo_longitude\":{\"type\":\"string\"},\"geo_region_name\":{\"type\":\"string\"},\"geo_timezone\":{\"type\":\"string\"},\"ip_address\":{\"type\":\"string\"},\"os\":{\"type\":\"string\"},\"os_name\":{\"type\":\"string\"},\"os_timezone\":{\"type\":\"string\"},\"page_url\":{\"type\":\"string\"},\"page_url_path\":{\"type\":\"string\"},\"referer_medium\":{\"type\":\"string\"},\"referer_url\":{\"type\":\"string\"},\"referer_url_port\":{\"type\":\"number\"},\"referer_url_scheme\":{\"type\":\"string\"},\"timestamp\":{\"type\":\"number\"},\"user_custom_id\":{\"type\":\"string\"},\"user_domain_id\":{\"type\":\"string\"},\"utm_campaign\":{\"type\":\"string\"},\"utm_content\":{\"type\":\"string\"},\"utm_medium\":{\"type\":\"string\"},\"utm_source\":{\"type\":\"string\"}}}}"

	assert.EqualValues(t, expected, result, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", expected, result))
}
