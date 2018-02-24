package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ResizeConfig struct {
	Name    string
	Width   int
	Height  int
	Quality int
}

func (config *ResizeConfig) GetOutputFileName(objKey string) string {
	return objKey
}

func (config *ResizeConfig) DestBucket() string {
	return "hngry-images"
}

func (config *ResizeConfig) DestKey() string {
	return ""
}

type SrcObject struct {
	Key          string
	Bucket       string
	TempfileName string
	Extension    string
	BasePath     string
	FileName     string
}

func (so *SrcObject) SetObject(bucket string, key string) {
	so.Bucket = bucket
	so.Key = key
	fileNameWithExt := filepath.Base(key)
	so.Extension = filepath.Ext(fileNameWithExt)

	so.TempfileName = fmt.Sprintf("/tmp/%s", fileNameWithExt)
	so.FileName = fileNameWithExt[0 : len(fileNameWithExt)-len(so.Extension)]

	parts := strings.Split(so.FileName, "-")
	so.BasePath = fmt.Sprintf("%s%s/", key[0:len(key)-len(fileNameWithExt)], strings.Join(parts, "/"))
}

// GetDestObjectKey Returns the nested Destination Object for a thumbnail
// Example. Original Key = p/images/f04902b4-ee4a-4f97-8c3b-bc1632f4ef6b.jpg; so.GetThumbTempFileName => p/images/f04902b4/ee4a/4f97/8c3b/bc1632f4ef6b/thumbnail-2x.jpg
func (so *SrcObject) GetDestObjectKey(thumbName string) string {
	return fmt.Sprintf("%s%s%s", so.BasePath, thumbName, so.Extension)
}

// IsValidImage returns true if key is supported
func (so *SrcObject) IsValidImage() bool {
	// TODO: Implement
	return true
}

// GetThumbTempFileName Returns the file name for the thumbnail.
// Example: Original Key = p/images/f04902b4-ee4a-4f97-8c3b-bc1632f4ef6b.jpg; so.GetThumbTempFileName => /tmp/f04902b4-ee4a-4f97-8c3b-bc1632f4ef6b_thumbnail-2x.jpg
func (so *SrcObject) GetThumbTempFileName(thumbName string) string {
	return fmt.Sprintf("/tmp/%s_%s%s", so.FileName, thumbName, so.Extension)
}

func initializeConfigs() []ResizeConfig {
	c := []ResizeConfig{}
	c = append(c, ResizeConfig{Name: "thumbnail", Width: 200, Height: 200, Quality: 95})
	c = append(c, ResizeConfig{Name: "thumbnail-2x", Width: 400, Height: 400, Quality: 80})
	c = append(c, ResizeConfig{Name: "gallery", Width: 600, Height: 600, Quality: 80})
	c = append(c, ResizeConfig{Name: "gallery-2x", Width: 1024, Height: 1024, Quality: 75})
	return c
}

var ResizeConfigs []ResizeConfig = initializeConfigs()
var svc *s3.S3 = initializeS3()

func initializeS3() *s3.S3 {
	return s3.New(session.New())
}

// ResizeImages is the main handler for lambda that kicks of resize of individual images
func ResizeImages(ctx context.Context, s3Event events.S3Event) error {
	// fmt.Printf("Context: %+v\n", ctx)
	// fmt.Printf("Event: %+v\n", s3Event)
	// fmt.Printf("ResizeConfigs: %+v\n", ResizeConfigs)
	var err error
	for _, record := range s3Event.Records {
		s3 := record.S3
		// fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
		srcObj := SrcObject{}
		srcObj.SetObject(s3.Bucket.Name, s3.Object.Key)

		if srcObj.IsValidImage() {
			err = resizeS3Object(srcObj)
		}
	}
	return err
}

func Dev() error {
	var err error
	srcObj := SrcObject{}
	srcObj.SetObject("hngry-original-images", "p/images/test/bbq.jpg")

	if srcObj.IsValidImage() {
		err = resizeS3Object(srcObj)
	}
	return err
}

func resizeS3Object(srcObj SrcObject) error {
	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(srcObj.Bucket),
		Key:    aws.String(srcObj.Key),
	})

	if err != nil {
		log.Fatal("Failed to get object", err)
	}

	file, err := os.Create(srcObj.TempfileName)
	if err != nil {
		log.Fatal("Failed to create file", err)
	}

	if _, err := io.Copy(file, result.Body); err != nil {
		log.Fatal("Failed to copy object to file. Num bytes %d", err)
	}
	result.Body.Close()
	file.Close()

	fmt.Printf("bucket = %v, objKey = %v Downloaded to file: %s\n", srcObj.Bucket, srcObj.Key, srcObj.TempfileName)

	for _, resizeConfig := range ResizeConfigs {
		err = createThumbnail(resizeConfig, srcObj)
	}

	return nil
}

func createThumbnail(config ResizeConfig, srcObj SrcObject) error {
	var err error
	outputFileName := srcObj.GetThumbTempFileName(config.Name)

	cmd := "convert"
	args := []string{"-thumbnail", fmt.Sprintf("%dx%d", config.Width, config.Height), "-quality", fmt.Sprintf("%d", config.Quality), srcObj.TempfileName, outputFileName}
	if err := exec.Command(cmd, args...).Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	err = uploadFileToS3(outputFileName, config.DestBucket(), srcObj.GetDestObjectKey(config.Name))
	return err
}

func uploadFileToS3(filename string, bucketName string, objKey string) error {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("err opening file: %s", err)
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size) // read file content to buffer

	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)

	fmt.Printf("Uploading file to bucket: %s, key %s", bucketName, objKey)

	resp, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objKey),
		Body:          fileBytes,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(fileType),
		ACL:           aws.String("public-read"),
	})

	if err != nil {
		fmt.Printf("bad response for key: %s error: %s\n response: %s", objKey, err, awsutil.StringValue(resp))
	}
	// fmt.Printf("response %s\n key: %s\n", awsutil.StringValue(resp), objKey)
	return err

}

func main() {
	lambda.Start(ResizeImages)
}
