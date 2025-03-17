package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Redis struct {
		URL string `yaml:"url"`
	} `yaml:"redis"`
	Simhash struct {
		Size        int   `yaml:"size"`
		ExpireAfter int64 `yaml:"expire_after"`
	} `yaml:"simhash"`
	Snapshots struct {
		NumberPerYear int `yaml:"number_per_year"`
	} `yaml:"snapshots"`
	Threads      int    `yaml:"threads"`
	CdxAuthToken string `yaml:"cdx_auth_token"`
	MaxDownloads int    `yaml:"max_downloads"`
	MaxErrors    int    `yaml:"max_errors"`
}

var AppConfig Config

func LoadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("Error reading config file: %v", err)
		return err
	}

	err = yaml.Unmarshal(data, &AppConfig)
	if err != nil {
		log.Printf("Error parsing config file: %v", err)
		return err
	}

	return nil
}
