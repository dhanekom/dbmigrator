package config

import (
	"log"
)

type AppConfig struct {
	Infolog  *log.Logger
	Errorlog *log.Logger
	AllowFix bool
}