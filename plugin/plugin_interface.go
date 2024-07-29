package plugin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/meta"
	"sync"
)

type Plugin interface {
	Connect(config *configor.Config, key string) (Plugin, error)
	Read(ctx context.Context, writer Plugin, tm *meta.TaskMeta) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Close()
}

var Plugins = make(map[string]Plugin, 0)

var lock sync.Mutex

func RegisterPlugin(pluginType string, p Plugin) error {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := Plugins[pluginType]; ok {
		errMsg := fmt.Sprintf("%s plugin already Register", pluginType)
		return errors.New(errMsg)
	}
	Plugins[pluginType] = p
	return nil
}

func GetPlugin(pluginType string) (Plugin, error) {
	lock.Lock()
	defer lock.Unlock()
	var p Plugin
	if v, ok := Plugins[pluginType]; !ok {
		errMsg := fmt.Sprintf("%s plugin not  Register please user func of init to RegisterPlugin", pluginType)
		return nil, errors.New(errMsg)
	} else {
		p = v
	}
	return p, nil
}

func init() {
	RegisterPlugin("mysql", NewMysqlPlugin())
}
