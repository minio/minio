package main

// +build ignore
func main() {}

//
//import (
//	"bytes"
//	"fmt"
//	"os"
//	"reflect"
//
//	"github.com/minio-io/minio/pkg/storage/donut/v1"
//)
//
//func main() {
//	fmt.Println("--start")
//
//	file, err := os.OpenFile("newfile", os.O_WRONLY|os.O_CREATE, 0666)
//	if err != nil {
//		panic(err)
//	}
//	donut := v1.Write(file)
//
//	gobHeader := v1.GobHeader{}
//	data := []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.")
//
//	dataBuffer := bytes.NewBuffer(data)
//	err = donut.Write(gobHeader, dataBuffer)
//	if err != nil {
//		panic(err)
//	}
//	file.Close()
//	fmt.Println("--closed")
//
//	fmt.Println("--verify")
//	stat, _ := os.Stat("newfile")
//	fileSize := stat.Size()
//
//	rfile, _ := os.OpenFile("newfile", os.O_RDONLY, 0666)
//	blockStart := make([]byte, 4)
//	blockStartCheck := []byte{'M', 'I', 'N', 'I'}
//
//	_, err = rfile.Read(blockStart)
//	if err != nil {
//		panic(err)
//	}
//
//	blockEnd := make([]byte, 4)
//	start := fileSize - 4
//	blockEndCheck := []byte{'I', 'N', 'I', 'M'}
//	rfile.ReadAt(blockEnd, start)
//	rfile.Close()
//
//	if !reflect.DeepEqual(blockStart, blockStartCheck) {
//		panic("Corrupted donut file")
//	}
//
//	if !reflect.DeepEqual(blockEnd, blockEndCheck) {
//		panic("Corrupted donut file")
//	}
//
//	fmt.Println("--verified")
//	fmt.Println("--end")
//}
