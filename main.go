package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type keyInfo struct {
	KeyName   string `json:"keyName"`
	KeyGroup  string `json:"keyBigName"`
	KeyTTL    int64  `json:"keyTTL"`
	KeySize   int64  `json:"keySize"`
	KeyIdle   int64  `json:"keyIdle"`
	RedisName string `json:"redisName"`
}

func scanRunner(instanceName string, messages chan []string) {
	c, err := redis.Dial("tcp", instanceName)
	if err != nil {
		fmt.Println("connect error", err)
		return
	}
	defer c.Close()
	var item int = 0
	for i := 0; i <= 20000; i++ {
		bytesKeyList, err := redis.Values(c.Do("scan", item, "count", 1000))
		if err != nil {
			fmt.Println("scan error", err)
			continue
		}
		keyList, _ := redis.Strings(bytesKeyList[1], nil)
		item, _ = redis.Int(bytesKeyList[0], nil)
		if item == 0 {
			break
		}
		messages <- keyList
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	close(messages)
}

func evalRunner(instanceName, scriptString string, messages chan []string, keyInfoChan chan keyInfo) {
	c, err := redis.Dial("tcp", instanceName)
	if err != nil {
		fmt.Println("connect error", err)
		return
	}
	defer c.Close()
	var getScript = redis.NewScript(0, scriptString)
	for tmpInfo := range messages {
		if len(tmpInfo) == 0 {
			continue
		}
		tmpData := make([]interface{}, len(tmpInfo))
		for k, v := range tmpInfo {
			tmpData[k] = v
		}
		reply, err := getScript.Do(c, tmpData...)
		if err != nil {
			fmt.Println("eval error", err)
			continue
		}
		keyInfoList, _ := redis.Values(reply, nil)
		keyGroupNameRegex := regexp.MustCompile(`([0-9])+|(:[A-Za-z0-9]*[0-9]+.*)`)
		keyNameRegex := regexp.MustCompile(`"`)
		for i := 0; i < len(tmpInfo); i++ {
			keyName := keyNameRegex.ReplaceAllString(tmpInfo[i], "\\\"")
			keyGroup := keyGroupNameRegex.ReplaceAllString(keyName, "*")
			keyTTL, _ := keyInfoList[i*3].(int64)
			keySize, _ := keyInfoList[i*3+1].(int64)
			keyIdle, _ := keyInfoList[i*3+2].(int64)
			keyInfoChan <- keyInfo{KeyName: keyName, KeyGroup: keyGroup, KeyTTL: keyTTL, KeySize: keySize, KeyIdle: keyIdle+keyTTL, RedisName: instanceName}

		}
	}
}
func main() {
	var evalWg sync.WaitGroup
	var wg sync.WaitGroup
	instanceName := os.Args[1]
	var scriptString string = `
	local data = {}
	local ttl = 0
	local size = 0
	local idle = 0
	for i,key in ipairs(ARGV) do
	    ttl = redis.call("TTL",key)
		table.insert(data,ttl)
		size =redis.call("MEMORY","USAGE", key)
		table.insert(data,size)
		idle = redis.call("OBJECT", "IDLETIME", key)
		table.insert(data,idle)
	end
	return data
	`
	keyInfoChan := make(chan keyInfo, 10)
	var ttlKeyCount int
	var bigKeyCount int
	messages := make(chan []string)

	go scanRunner(instanceName, messages)

	for i := 0; i <= 10; i++ {
		evalWg.Add(1)
		go func() {
			evalRunner(instanceName, scriptString, messages, keyInfoChan)
			defer evalWg.Done()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		f, err := os.Create("keys.txt")
		if err != nil {
			panic(err)
		}
		w := bufio.NewWriter(f)
		defer f.Close()
		i := 0
		for keyinfo := range keyInfoChan {
			if keyinfo.KeyTTL == -1 {
				ttlKeyCount++
			}
			if keyinfo.KeySize >= 1000 {
				bigKeyCount++
			}
			i++
			keyinfoString, _ := json.Marshal(keyinfo)
			_, err := w.WriteString(fmt.Sprintf("{\"index\":{\"_id\":\"%d\"}}\n%s\n", i, string(keyinfoString)))
			if err != nil {
				panic(err)
			}
		}
		w.Flush()
	}()
	evalWg.Wait()
	close(keyInfoChan)
	wg.Wait()
	fmt.Println(ttlKeyCount, bigKeyCount)

}
