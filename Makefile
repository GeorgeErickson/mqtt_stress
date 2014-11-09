deps:
	@go get git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git
	@go get code.google.com/p/go-uuid/uuid

simple:
	go run bin/simple.go
