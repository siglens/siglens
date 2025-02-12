package alertsHandler

import (
	"testing"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/stretchr/testify/assert"
)

func Test_ValidateAlertTypeAndQuery(t *testing.T) {
	assertIsValidLogAlert(t, false, "")
	assertIsValidLogAlert(t, false, "foo=bar")
	assertIsValidLogAlert(t, true, "foo=bar | stats count")
	assertIsValidLogAlert(t, true, "foo=bar | stats count by weekday")
	assertIsValidLogAlert(t, true, "foo=bar | stats count by weekday, alpha, beta")
	assertIsValidLogAlert(t, true, "* | stats count by weekday, alpha, beta")
	assertIsValidLogAlert(t, true, "foo=bar | stats min(latency)")
}

func assertIsValidLogAlert(t *testing.T, isValid bool, splunkQuery string) {
	t.Helper()

	alert := &alertutils.AlertDetails{
		AlertConfig: alertutils.AlertConfig{
			AlertType: alertutils.AlertTypeLogs,
			QueryParams: alertutils.QueryParams{
				QueryLanguage: "Splunk QL",
				QueryText:     splunkQuery,
			},
		},
	}

	_, err := validateAlertTypeAndQuery(alert)
	if isValid {
		assert.NoError(t, err)
	} else {
		assert.Error(t, err)
	}
}
