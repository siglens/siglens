package spl

func toStringSlice(input interface{}) []string {
	if input == nil {
		return nil
	}
	if slice, ok := input.([]string); ok {
		return slice
	}
	return nil
}

func captureIdentifiers(input interface{}) interface{} {
	return input
}
