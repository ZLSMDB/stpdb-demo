package main

import (
	"fmt"
	"log"

	db "github.com/ZLSMDB/stpdb-demo/src"
)

func main() {
	// 初始化数据库
	ldb, err := db.NewLevelDB("test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer ldb.Close()

	for i := 0; i < 1000; i++ {
		key := []byte("prefix_" + fmt.Sprint(i))
		value := []byte("value" + string(i))
		err := ldb.Put(key, value)
		if err != nil {
			log.Fatal(err)
		}
	}

	// 导出前缀为 "prefix_" 的所有键值对到文件中
	err = ldb.ExportPrefixToFile([]byte("prefix_"))
	if err != nil {
		log.Fatal(err)
	}

	// 导入文件中的键值对到数据库
	// err = ldb.ImportFromFile("prefix_.stp")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println("Imported key-value pairs from file.")

	// 修改文件中的某个键值对
	err = ldb.ModifyValueInFile("prefix_.stp", "prefix_1", "new_value2")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Modified value in file.")
}
