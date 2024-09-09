package src

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	db   *leveldb.DB
	lock sync.RWMutex
}

// NewLevelDB initializes and returns a new LevelDB instance.
func NewLevelDB(path string) (*LevelDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDB{
		db: db,
	}, nil
}

// Close closes the LevelDB database.
func (ldb *LevelDB) Close() error {
	ldb.lock.Lock()
	defer ldb.lock.Unlock()

	return ldb.db.Close()
}

// Put stores a key-value pair in the database.
func (ldb *LevelDB) Put(key, value []byte) error {
	ldb.lock.Lock()
	defer ldb.lock.Unlock()

	return ldb.db.Put(key, value, nil)
}

// Get retrieves the value for a given key.
func (ldb *LevelDB) Get(key []byte) ([]byte, error) {
	ldb.lock.RLock()
	defer ldb.lock.RUnlock()

	value, err := ldb.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Delete removes a key-value pair from the database.
func (ldb *LevelDB) Delete(key []byte) error {
	ldb.lock.Lock()
	defer ldb.lock.Unlock()

	return ldb.db.Delete(key, nil)
}

// ExportPrefixToFile exports all key-value pairs with the same prefix to a file.
func (ldb *LevelDB) ExportPrefixToFile(prefix []byte) error {
	ldb.lock.RLock()
	defer ldb.lock.RUnlock()

	start := time.Now()

	filename := fmt.Sprintf("%s.stp", prefix)
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	iter := ldb.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		line := fmt.Sprintf("#%s=%s;\n", key[len(prefix)+1:], value)
		// line := fmt.Sprintf("%s:%s\n", key, value)
		if _, err := file.WriteString(line); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}
	// 计算导出时间
	elapsed := time.Since(start)
	fmt.Printf("Data with prefix '%s' exported to %s successfully in %v seconds.\n", prefix, filename, elapsed.Seconds())
	return nil
}

// // ImportFromFile loads key-value pairs from a file into the LevelDB.
// func (ldb *LevelDB) ImportFromFile(filename string) error {
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		parts := strings.SplitN(line, ":", 2)
// 		if len(parts) != 2 {
// 			continue // Skip malformed lines
// 		}
// 		key := []byte(parts[0])
// 		value := []byte(parts[1])
// 		ldb.Put(key, value)
// 	}

// 	return scanner.Err()
// }

// ParseStepFile 解析 STEP 文件，返回所有的键值对
func (ldb *LevelDB) ParseStepFile(filepath string) (map[string]string, error) {
	// 记录解析开始时间
	start := time.Now()
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 存储解析出的键值对
	parsedData := make(map[string]string)
	// 正则表达式匹配 STEP 文件行
	re := regexp.MustCompile(`#(\d+)=(.+);`)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if len(matches) != 3 {
			continue // 跳过不匹配的行
		}
		key := matches[1]
		value := matches[2]
		parsedData[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	// 记录解析结束时间并计算耗时
	elapsed := time.Since(start)
	fmt.Printf("Time taken to parse step data to kv pairs: %v seconds\n", elapsed.Seconds())
	return parsedData, nil
}

// StoreParsedData 存储解析出的键值对到 LevelDB 中
func (ldb *LevelDB) StoreParsedData(data map[string]string, stepFileName string) error {
	// 记录存储开始时间
	start := time.Now()

	for key, value := range data {
		// 为 key 添加文件名前缀
		fullKey := stepFileName + "_" + key
		err := ldb.Put([]byte(fullKey), []byte(value))
		if err != nil {
			return err
		}
	}

	// 记录存储结束时间并计算耗时
	elapsed := time.Since(start)
	fmt.Printf("Time taken to store data in LevelDB: %v seconds\n", elapsed.Seconds())
	return nil
}

// ModifyValueInFile modifies the value of a specific key in a file and writes the changes back.
func (ldb *LevelDB) ModifyValueInFile(filename string, targetKey, newValue string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	keyFound := false

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		value := parts[1]
		if key == targetKey {
			value = newValue
			keyFound = true
		}
		lines = append(lines, fmt.Sprintf("%s:%s", key, value))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if !keyFound {
		// 如果未找到目标键，则添加新的键值对
		lines = append(lines, fmt.Sprintf("%s:%s", targetKey, newValue))
	}

	// 将修改后的内容写回文件
	file, err = os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, line := range lines {
		_, err := file.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}
