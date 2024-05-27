Install the necessary dependencies using following commnds:
go get github.com/fsnotify/fsnotify
go get github.com/spf13/viper
Runing the application : 
go run main.go -config configuration.yaml

Setup will continuously monitor the target dir , process files concurrently, and update the fileData.json with the size of each file
