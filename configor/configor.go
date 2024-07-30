package configor

import (
	"github.com/chengcxy/goshift/utils"
	"os"
	"path"
	"strings"
)

type Config struct {
	ConfigPath string
	Env        string
	Conf       map[string]interface{}
	UsedEnv    bool
}

func NewConfig(ConfigPath, Env string, UsedEnv bool) *Config {
	c := &Config{
		ConfigPath: ConfigPath,
		Env:        Env,
		UsedEnv:    UsedEnv,
	}
	conf, _ := c.getJsonConfig()
	c.Conf = conf
	return c
}

func (c *Config) getJsonConfig() (map[string]interface{}, error) {
	json_file := path.Join(c.ConfigPath, c.Env+".json")
	m, err := utils.ParseJsonFile(json_file)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (c *Config) parseValueByEnv(value interface{}) interface{} {
	if c.UsedEnv {
		switch value.(type) {
		case string:
			k := value.(string)
			v := os.Getenv(k)
			if v == "" {
				return value
			} else {
				return v
			}
		case map[string]interface{}:
			temp := make(map[string]interface{})
			for k, v := range value.(map[string]interface{}) {
				temp[k] = c.parseValueByEnv(v)
			}
			return temp
		case []interface{}:
			temp := make([]interface{}, 0)
			for _, v := range value.([]interface{}) {
				temp = append(temp, c.parseValueByEnv(v))
			}
			return temp
		default:
			return value
		}
	} else {
		return value
	}
}

func (c *Config) getConfig(conf interface{}, key string) (interface{}, bool) {
	mp := conf.(map[string]interface{})
	value, ok := mp[key]
	if ok {
		return c.parseValueByEnv(value), ok
	}
	isReparse := strings.Contains(key, ".")
	if isReparse {
		keys := strings.Split(key, ".")
		for len(keys) > 0 {
			k := keys[0]
			keys = keys[1:]
			strKeys := strings.Join(keys, ".")
			value, ok := mp[k]
			if ok {
				return c.getConfig(value, strKeys)
			}
			return nil, false
		}
	}
	return nil, false
}

// json key like "a.b.c"
func (c *Config) Get(key string) (interface{}, bool) {
	return c.getConfig(c.Conf, key)
}
