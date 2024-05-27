package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type FileData struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type Config struct {
	TargetDirectory  string
	StorageLocation  string
	ConcurrencyLevel int
}

func main() {
	// Setup command line flags
	configPath := flag.String("config", "configuration.yaml", "path to config file")
	flag.Parse()

	// Load configuration
	var config Config
	viper.SetConfigFile(*configPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("Error parsing config file: %v", err)
	}

	// Create a watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	// Add the target directory to the watcher
	err = watcher.Add(config.TargetDirectory)
	if err != nil {
		log.Fatal(err)
	}

	// Channel for file paths to be processed
	fileChan := make(chan string, config.ConcurrencyLevel)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < config.ConcurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range fileChan {
				processFile(path, config.StorageLocation)
			}
		}()
	}

	// Monitor the directory
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
					fileChan <- event.Name
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	// Wait for the goroutines to finish
	wg.Wait()
	close(fileChan)
}

func processFile(path string, storageLocation string) {
	// Read file content
	info, err := os.Stat(path)
	if err != nil {
		log.Printf("Failed to stat file %s: %v", path, err)
		return
	}

	// Create file data
	fileData := FileData{
		Path: path,
		Size: info.Size(),
	}

	// Read existing data
	var fileDataList []FileData
	if _, err := os.Stat(storageLocation); err == nil {
		data, err := ioutil.ReadFile(storageLocation)
		if err != nil {
			log.Printf("Failed to read storage file: %v", err)
			return
		}
		if err := json.Unmarshal(data, &fileDataList); err != nil {
			log.Printf("Failed to unmarshal storage file: %v", err)
			return
		}
	}

	// Update file data
	fileDataList = append(fileDataList, fileData)

	// Write updated data
	data, err := json.MarshalIndent(fileDataList, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal data: %v", err)
		return
	}
	if err := ioutil.WriteFile(storageLocation, data, 0644); err != nil {
		log.Printf("Failed to write storage file: %v", err)
	}
}
