package main

import (
	"fmt"
	"time"
	"audit" // Ensure audit.go is in the same package or imported correctly
)

func main() {
	logger := audit.NewAuditLogger("audit_log.json")

	// Create an audit event
	err := logger.CreateAuditEvent("JohnDoe", "User logged in", "Login successful", time.Now().Unix(), 1345678908)
	if err != nil {
		fmt.Println("Failed to write log:", err)
	}

	// Read audit events within the last 24 hours
	startTime := time.Now().Add(-24 * time.Hour).Unix()
	endTime := time.Now().Unix()
	events, err := logger.ReadAuditEvent(1345678908, startTime, endTime)
	if err != nil {
		fmt.Println("Failed to read logs:", err)
	}

	// Print retrieved logs
	for _, event := range events {
		fmt.Printf("Log: %+v\n", event)
	}
}
