package db

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"

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
		line := fmt.Sprintf("%s:%s\n", key, value)
		if _, err := file.WriteString(line); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}
	return nil
}

// ImportFromFile loads key-value pairs from a file into the LevelDB.
func (ldb *LevelDB) ImportFromFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue // Skip malformed lines
		}
		key := []byte(parts[0])
		value := []byte(parts[1])
		ldb.Put(key, value)
	}

	return scanner.Err()
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
