package config

import (
	"fmt"
	"log"
)

type AppConfig struct {
	Infolog  *log.Logger
	Errorlog *log.Logger
	Verbose  bool
	AllowFix bool
}

func (a *AppConfig)  LogVerboseLn(v ...any) {
	if a.Verbose {
		fmt.Println(v...)
	}
}

func (a *AppConfig) LogVerbose(v ...any) {
	if a.Verbose {
		fmt.Print(v...)
	}
}

func (a *AppConfig) LogVerbosef(format string, v ...any) {
	if a.Verbose {
		fmt.Printf(format, v...)
	}
}