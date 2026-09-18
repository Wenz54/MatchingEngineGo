package main

import (
	"flag"
	"fmt"
	"log"

	"petProjectMatchingEngine/internal/config"
	"petProjectMatchingEngine/internal/shard"
)

func main() {
	cfgPath := flag.String("config", "./configs/dev.json", "path to config")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if err := shard.VerifyReplay(cfg); err != nil {
		log.Fatalf("replay verification failed: %v", err)
	}

	fmt.Println("replay verification passed")
}
