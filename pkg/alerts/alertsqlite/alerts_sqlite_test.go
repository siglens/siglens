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

package alertsqlite

import (
	"testing"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/stretchr/testify/assert"
)

func Test_resolveTemplate(t *testing.T) {
	template := `{{alert_rule_name}}: "{{query_string}}" ({{queryLanguage}}) {{condition}}`

	actual := resolveTemplate(template, "alert1", alertutils.IsAbove, 42, "Splunk QL", "* | stats count")
	assert.Equal(t, `alert1: "* | stats count" (Splunk QL) is above 42`, actual)

	actual = resolveTemplate(template, "alert2", alertutils.IsBelow, 42, "Splunk QL", "* | stats count")
	assert.Equal(t, `alert2: "* | stats count" (Splunk QL) is below 42`, actual)

	actual = resolveTemplate(template, "alert3", alertutils.IsEqualTo, 42, "Splunk QL", "* | stats count")
	assert.Equal(t, `alert3: "* | stats count" (Splunk QL) is equal to 42`, actual)

	actual = resolveTemplate(template, "alert4", alertutils.IsNotEqualTo, 42, "Splunk QL", "* | stats count")
	assert.Equal(t, `alert4: "* | stats count" (Splunk QL) is not equal to 42`, actual)

	actual = resolveTemplate("static message", "alert5", alertutils.IsAbove, 42, "Splunk QL", "* | stats count")
	assert.Equal(t, "static message", actual)
}
