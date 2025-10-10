package server

type Config struct {
	entries map[string]string
}

func NewConfig(entries map[string]string) *Config {
	return &Config{
		entries: entries,
	}
}

func (c *Config) Get(key string) string {
	value, ok := c.entries[key]

	if ok {
		return value
	}

	return ""
}

func (c *Config) Set(key, value string) {
	c.entries[key] = value
}
