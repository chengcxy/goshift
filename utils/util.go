package utils

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func Md5(str string) string {
	w := md5.New()
	io.WriteString(w, str)
	md5str := fmt.Sprintf("%x", w.Sum(nil))
	return md5str
}

func ParseJsonFile(json_file string) (map[string]interface{}, error) {
	file, err := os.Open(json_file)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil {
		return nil, err
	}
	m := f.(map[string]interface{})
	return m, nil

}
