package kafka_go

import (
	"log"
	"os"
)

var (
	Logger = log.New(os.Stdout, "[kafka-go]", log.LstdFlags)
)
