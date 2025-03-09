package main

import (
	"fmt"
	"time"

	"siglens/pkg/audit" // Adjust the import path based on your project structure.
)

func main() {
	// Create a couple of audit events for demonstration.
	now := time.Now().Unix()

	err := audit.CreateAuditEvent("JohnDoe", "User logged in", "Successful login", now, 1345678908)
	if err != nil {
		fmt.Printf("Error creating audit event: %v\n", err)
	} else {
		fmt.Println("Audit event created successfully!")
	}

	// Wait a couple seconds
	time.Sleep(2 * time.Second)

	err = audit.CreateAuditEvent("JohnDoe", "User deleted index", "Index: customers", time.Now().Unix(), 1345678908)
	if err != nil {
		fmt.Printf("Error creating audit event: %v\n", err)
	} else {
		fmt.Println("Audit event created successfully!")
	}

	// Define a time range that includes our events.
	startTime := now - 10
	endTime := time.Now().Unix() + 10

	// Retrieve events for the given organization within the time range.
	events, err := audit.ReadAuditEvent(1345678908, startTime, endTime)
	if err != nil {
		fmt.Printf("Error reading audit events: %v\n", err)
		return
	}

	fmt.Println("Audit events:")
	for _, e := range events {
		fmt.Printf("%+v\n", e)
	}
}