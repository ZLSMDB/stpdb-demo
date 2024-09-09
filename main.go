package main

import (
	"fmt"
	"log"
	"path/filepath"
	db "github.com/ZLSMDB/stpdb-demo/src"
)

// func main() {
// 	// 初始化数据库
// 	ldb, err := db.NewLevelDB("test.db")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer ldb.Close()

// 	for i := 0; i < 1000; i++ {
// 		key := []byte("prefix_" + fmt.Sprint(i))
// 		value := []byte("value" + string(i))
// 		err := ldb.Put(key, value)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 	}

// 	// 导出前缀为 "prefix_" 的所有键值对到文件中
// 	err = ldb.ExportPrefixToFile([]byte("prefix_"))
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	// 导入文件中的键值对到数据库
// 	// err = ldb.ImportFromFile("prefix_.stp")
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// log.Println("Imported key-value pairs from file.")

// 	// 修改文件中的某个键值对
// 	err = ldb.ModifyValueInFile("prefix_.stp", "prefix_1", "new_value2")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	log.Println("Modified value in file.")
// }

func main() {
	// 初始化 LevelDB
	ldb, err := db.NewLevelDB("test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer ldb.Close()

	stepFilePath := "/home/step192/step_data/5604121-C01_20171023.stp" // 你的 STEP 文件路径
	// 提取 STEP 文件名（不含路径和扩展名）
	stepFileName := filepath.Base(stepFilePath)
	stepFileName = stepFileName[:len(stepFileName)-len(filepath.Ext(stepFileName))]

	// 解析 STEP 文件
	parsedData, err := ldb.ParseStepFile(stepFilePath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("STEP file parsed successfully.")

	// 将解析后的数据存储到 LevelDB
	err = ldb.StoreParsedData(parsedData, stepFileName)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Parsed data stored in LevelDB successfully.")

	// 导出具有特定前缀的数据到文件
	prefix := "5604121-C01_20171023"
	err = ldb.ExportPrefixToFile([]byte(prefix))
	if err != nil {
		log.Fatal(err)
	}
}