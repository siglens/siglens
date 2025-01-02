package utils

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
)

func OnFileChange(files []string, callback func()) error {
	if len(files) == 0 {
		return nil
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("OnFileChange: Error creating file watcher: %v", err)
	}

	// Add the files to the watcher
	for _, file := range files {
		err = watcher.Add(file)
		if err != nil {
			return fmt.Errorf("OnFileChange: Error adding file to watcher: %v", err)
		}
	}

	// Watch for changes
	go func() {
		defer watcher.Close()
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					log.Errorf("OnFileChange: Watcher events channel closed for files: %v", files)
					return
				}
				if event.Has(fsnotify.Write) {
					log.Infof("OnFileChange: File %v changed", event.Name)
					callback()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Errorf("OnFileChange: Watcher errors channel closed for files: %v", files)
					return
				}
				log.Errorf("OnFileChange: Watcher error=%v for files %v", err, files)
				return
			}
		}
	}()

	return err
}
