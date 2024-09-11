package src

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"context"

	"github.com/minio/minio-go/v7"
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

// ExportPrefixToFile 将所有具有相同前缀的键值对导出到文件中。
func (ldb *LevelDB) ExportPrefixToFile(prefix []byte) error {
	ldb.lock.RLock()
	defer ldb.lock.RUnlock()

	start := time.Now()

	filename := fmt.Sprintf("%s.stp", prefix)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	iter := ldb.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if len(key) <= len(prefix)+1 {
			continue
		}
		line := fmt.Sprintf("#%s=%s;\n", key[len(prefix)+1:], value)
		if _, err := writer.WriteString(line); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("迭代器错误: %w", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("前缀为 '%s' 的数据成功导出到 %s，用时 %.2f 秒。\n", prefix, filename, elapsed.Seconds())
	return nil
}

func (ldb *LevelDB) ExportPrefixToMinIO(prefix []byte, minioClient *minio.Client, bucketName, objectName string) error {
	ldb.lock.RLock()
	defer ldb.lock.RUnlock()

	start := time.Now()

	// 创建本地临时文件
	filename := fmt.Sprintf("%s.stp", prefix)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	iter := ldb.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if len(key) <= len(prefix)+1 {
			continue
		}
		line := fmt.Sprintf("#%s=%s;\n", key[len(prefix)+1:], value)
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("迭代器错误: %w", err)
	}

	// 将文件上传到 MinIO
	ctx := context.Background()
	_, err = minioClient.FPutObject(ctx, bucketName, objectName, filename, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("文件上传到 MinIO 失败: %w", err)
	}

	// 计算导出时间
	elapsed := time.Since(start)
	fmt.Printf("前缀为'%s'的数据成功导出到MinIO中'%s/%s'，用时 %.2f 秒。\n", prefix, bucketName, objectName, elapsed.Seconds())
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
func (ldb *LevelDB) ParseStepFile(filepath string) ([]string, [][]byte, error) {
	// 记录解析开始时间
	start := time.Now()

	var keys []string
	var values [][]byte

	file, err := os.Open(filepath)
	if err != nil {
		return nil, nil,  err
	}
	defer file.Close()
	// 正则表达式匹配 STEP 文件行
	re := regexp.MustCompile(`#(\d+)=(.+);`)

	// 使用 bufio.Reader 替代 bufio.Scanner
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				break // 文件读取完毕
			}
			return nil, nil, err
		}

		// 处理每一行
		line = line[:len(line)-1] // 去掉行末的换行符
		matches := re.FindStringSubmatch(line)
		if len(matches) != 3 {
			continue // 跳过不匹配的行
		}
		key := matches[1]
		value := matches[2]
		keys = append(keys, key)
		values = append(values, []byte(value))
	}

	// 记录解析结束时间并计算耗时
	elapsed := time.Since(start)
	fmt.Printf("将step文件解析成kv对的时间: %v seconds\n", elapsed.Seconds())
	return keys, values, nil
}

// StoreParsedData 存储解析出的键值对到 LevelDB 中
func (ldb *LevelDB) StoreParsedData(stepFileName string, keys []string, values [][]byte) error {
	// 记录存储开始时间
	start := time.Now()

	const batchSize = 1000 // 设置适当的批量大小
	batch := new(leveldb.Batch)

	for i := range keys {
		// 为 key 添加文件名前缀
		fullKey := stepFileName + "_" + keys[i]
		batch.Put([]byte(fullKey), values[i])
		if (i+1)%batchSize == 0 {
			if err := ldb.db.Write(batch, nil); err != nil {
				return err
			}
			// 清空 batch
			batch = new(leveldb.Batch)
		}
	}

	// 写入剩余的操作
	if batch.Len() > 0 {
		if err := ldb.db.Write(batch, nil); err != nil {
			return err
		}
	}

	// 记录存储结束时间并计算耗时
	elapsed := time.Since(start)
	fmt.Printf("将解析后的 kv 对存进 LevelDB 的时间: %v seconds\n", elapsed.Seconds())
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
