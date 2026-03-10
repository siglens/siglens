// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
