package utils

import (
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

func WatchFileChanges(files []string, refreshIntervalSeconds int, callback func()) error {
	if len(files) == 0 {
		return nil
	}

	// Track last hash of each file
	lastHashes := make(map[string]string)

	// Compute hash of a file
	hashFile := func(filePath string) (string, error) {
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			return "", err
		}

		hash := sha256.New()
		_, err = hash.Write(fileBytes)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%x", hash.Sum(nil)), nil
	}

	// Check new hashes every {refreshIntervalSeconds} seconds
	ticker := time.NewTicker(time.Duration(refreshIntervalSeconds) * time.Second)

	go func() {
		defer ticker.Stop()
		for {
			<-ticker.C
			for _, file := range files {
				currentHash, err := hashFile(file)
				if err != nil {
					log.Errorf("OnFileChange: Error hashing file %v: %v", file, err)
					continue
				}

				// Check if the hash has changed
				lastHash, exists := lastHashes[file]
				if !exists || currentHash != lastHash {
					log.Infof("OnFileChange: File %v has changed", file)
					callback()
					lastHashes[file] = currentHash
				}
			}
		}
	}()

	return nil
}
