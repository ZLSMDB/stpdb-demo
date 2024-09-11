package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"
	"path/filepath"

	db "github.com/ZLSMDB/stpdb-demo/src"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// 初始化 LevelDB
	ldb, err := db.NewLevelDB("test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer ldb.Close()

	// STEP 文件路径
	stepFilePath := "/home/step192/step_data/1000410-28L.stp"

	// 提取 STEP 文件名（不含路径和扩展名）
	stepFileName := filepath.Base(stepFilePath)
	stepFileName = stepFileName[:len(stepFileName)-len(filepath.Ext(stepFileName))]

	// 解析 STEP 文件
	keys, values, err := ldb.ParseStepFile(stepFilePath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("step文件解析完成.")

	// 将解析后的数据存储到 LevelDB
	err = ldb.StoreParsedData(stepFileName, keys, values)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("step文件对应的kv存储完成.")

	// // 导出具有特定前缀的数据到文件
	// prefix := "5604121-C01_20171023"
	// err = ldb.ExportPrefixToFile([]byte(prefix))
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// 设置 MinIO 客户端参数
	endpoint := "10.176.34.130:9006"
	accessKeyID := "admin"
	secretAccessKey := "admin123456"
	useSSL := false

	// 初始化 MinIO 客户端
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatal("MinIO 客户端初始化失败:", err)
	}

	prefix := "1000410-28L"
	// MinIO 存储桶和对象名称
	bucketName := "1000410-28l" // 存储桶名称好像需要符合规则
	ctx := context.Background()
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		log.Fatalln(err)
	}

	// 创建bucket
	if !exists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("存储桶%s已创建\n", bucketName)
	} else {
		fmt.Printf("存储桶%s已存在\n", bucketName)
	}

	// 获取存储桶中现有的对象
	objectCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true})
	var fileCount int

	for obj := range objectCh {
		if obj.Err != nil {
			log.Fatalln(obj.Err)
		}
		fileCount++
	}
	// 生成新的对象名称
	objectName := fmt.Sprintf("%s_v%d.stp", prefix, fileCount+1)

	// 调用导出函数
	err = ldb.ExportPrefixToMinIO([]byte(prefix), minioClient, bucketName, objectName)
	if err != nil {
		fmt.Println("导出失败:", err)
	} else {
		fmt.Println("导出成功")
	}

	// 从minio中下载stp文件到本地
	localFilePath := "/home/step192/step_data/" + objectName // 本地保存路径
	ctx = context.Background()
	err = downloadFileFromMinIO(ctx, minioClient, bucketName, objectName, localFilePath)
	if err != nil {
		fmt.Println("下载失败:", err)
	} else {
		fmt.Println("下载成功")
	}
}

// 从 MinIO 下载文件并保存到本地
func downloadFileFromMinIO(ctx context.Context, minioClient *minio.Client, bucketName, objectName, localFilePath string) error {
	start := time.Now()
	object, err := minioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("从 MinIO 获取对象失败: %v", err)
	}
	defer object.Close()

	file, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("创建本地文件失败: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(file, object)
	if err != nil {
		return fmt.Errorf("将文件内容复制到本地文件失败: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf("文件%s从MinIO下载并保存到本地路径%s, 用时 %.2f 秒\n", objectName, localFilePath, elapsed.Seconds())
	return nil
}
