package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strings"
)

const server_addr = "192.168.1.2:8080"

func main() {
	for {
		scanner := bufio.NewScanner(os.Stdin)

		fmt.Println("enter your order: ")
		scanner.Scan()
		order := scanner.Text()
		if strings.EqualFold(order, "Info") {
			post, errPost := http.Post("http://"+server_addr+"/Info", "application/octet-stream", bytes.NewBuffer([]byte(order)))
			if errPost != nil {
				fmt.Println(errPost)
			} else {
				respBody, errReadAllSendToServer := ioutil.ReadAll(post.Body)
				if errReadAllSendToServer != nil {
					fmt.Println(errReadAllSendToServer.Error())
				}

				//charsArr := strings.Split(string(respBody)[strings.LastIndex(string(respBody), "[")+1:strings.LastIndex(string(respBody), "]")], " ")
				//var byteArr []byte
				//for _, s := range charsArr {
				//	myByte, _ := strconv.Atoi(s)
				//	byteArr = append(byteArr, byte(myByte))
				//}
				msg := fmt.Sprintf("%s", respBody)
				fmt.Println("main server says: ", msg)
			}
		} else if strings.Contains(order, "insert") {
			filePath := strings.Split(order, "\"")[1]
			fileOpened, errFileOpened := os.Open(filePath)
			if errFileOpened != nil {
				fmt.Println(errFileOpened.Error())
				return
			}

			stat, errFileState := fileOpened.Stat()
			if errFileState != nil {
				fmt.Println(errFileState.Error())
			}
			fmt.Println(stat)

			CsvBody := &bytes.Buffer{}
			CsvWriter := multipart.NewWriter(CsvBody)
			if part, errCreateFromFile := CsvWriter.CreateFormFile("file", filePath); errCreateFromFile != nil {
				fmt.Println(errCreateFromFile.Error())
			} else {
				_, errCopy := io.Copy(part, fileOpened)
				if errCopy != nil {
					fmt.Println(errCopy.Error())
				}
				CsvWriter.Close()

				if req, reqErr := http.NewRequest("POST", "http://"+server_addr+"/uploadCsv", CsvBody); reqErr != nil {
					fmt.Println(reqErr.Error())
				} else {
					req.Header.Set("Content-Type", CsvWriter.FormDataContentType())
					client := &http.Client{}
					if resp, errDo := client.Do(req); errDo != nil {
						fmt.Println(errDo.Error())
					} else {
						//defer resp.Body.Close()
						respBody, errReadAllResponce := ioutil.ReadAll(resp.Body)
						if errReadAllResponce != nil {
							fmt.Println(errReadAllResponce.Error())
						}
						fmt.Println("main server says: ", string(respBody))
					}

				}
			}
		} else if strings.Contains(order, "get") {
			message := order

			if listner, ListenErr := net.Listen("tcp", ":9090"); ListenErr != nil {
				fmt.Println(ListenErr)
			} else {
				post, errPost := http.Post("http://"+server_addr+"/getAllData", "application/octet-stream", bytes.NewBuffer([]byte(message)))
				if errPost != nil {
					fmt.Println(errPost)
				} else {
					defer post.Body.Close()

					respBody, errReadAllSendToServer := ioutil.ReadAll(post.Body)
					if errReadAllSendToServer != nil {
						fmt.Println(errReadAllSendToServer.Error())
					}
					fmt.Println("main server says: ", string(respBody))

					//http.HandleFunc("/getAllDataRequested", DataGetterHandler)
					//fmt.Println("order sent successfully to the server")

					accept, AcceptErr := listner.Accept()
					if AcceptErr != nil {
						fmt.Println(AcceptErr.Error())
					} else {
						CsvBuf := make([]byte, 1024*1024*24)
						n, ReadAllErr := accept.Read(CsvBuf)
						if ReadAllErr != nil {
							fmt.Println(ReadAllErr.Error())
						}
						fmt.Println(n)

						fmt.Println(string(CsvBuf[:1000]))
						CsvContent := csv.NewReader(bytes.NewReader(CsvBuf[:n]))
						file, createFileErr := os.Create("getResult.csv")
						if createFileErr != nil {
							fmt.Println(createFileErr.Error())
						}

						defer file.Close()

						writer := csv.NewWriter(file)
						data, _ := CsvContent.ReadAll()
						//fmt.Println(data[0])
						for _, row := range data {
							err := writer.Write(row)
							if err != nil {
								panic(err)
							}
						}

						writer.Flush()
					}
				}
			}
		} else if strings.Contains(order, "put") {
			conn, err := net.Dial("tcp", server_addr)
			if err != nil {
				fmt.Println(err)
				return
			}
			defer conn.Close()

			filePath := strings.Split(order, "\"")[1]
			fileOpened, errFileOpened := os.Open(filePath)
			if errFileOpened != nil {
				fmt.Println(errFileOpened.Error())
				return
			}
			//fileInfo, _ := fileOpened.Stat()
			//sizeBuf := make([]byte, 8)
			//binary.LittleEndian.PutUint64(sizeBuf, uint64(fileInfo.Size()))
			//_, errWriteBufSize := conn.Write([]byte("put"))
			//if errWriteBufSize != nil {
			//	fmt.Println(errWriteBufSize.Error())
			//	return
			//}

			//messageByte := append([]byte("put "), file...)
			if _, errSendFile := conn.Write([]byte("put file")); errSendFile != nil {
				fmt.Println(errSendFile.Error())
			} else {
				fmt.Println("sent msg")
				defer conn.Close()
			}
			if connv, errv := net.Dial("tcp", server_addr); errv != nil {
				fmt.Println(errv.Error())
			} else {
				_, errCopy := io.Copy(connv, fileOpened)
				if errCopy != nil {
					fmt.Println(errCopy.Error())
				}
				fmt.Println("file content sent successfully")
			}

		} else if strings.Contains(order, "update") {
			filePath := strings.Split(order, "\"")[1]
			fileOpened, errFileOpened := os.Open(filePath)
			if errFileOpened != nil {
				fmt.Println(errFileOpened.Error())
				return
			}

			stat, errFileState := fileOpened.Stat()
			if errFileState != nil {
				fmt.Println(errFileState.Error())
			}
			fmt.Println(stat)

			CsvBody := &bytes.Buffer{}
			CsvWriter := multipart.NewWriter(CsvBody)
			if part, errCreateFromFile := CsvWriter.CreateFormFile("file", filePath); errCreateFromFile != nil {
				fmt.Println(errCreateFromFile.Error())
			} else {
				_, errCopy := io.Copy(part, fileOpened)
				if errCopy != nil {
					fmt.Println(errCopy.Error())
				}
				CsvWriter.Close()

				if req, reqErr := http.NewRequest("POST", "http://"+server_addr+"/updateData", CsvBody); reqErr != nil {
					fmt.Println(reqErr.Error())
				} else {
					req.Header.Set("Content-Type", CsvWriter.FormDataContentType())
					client := &http.Client{}
					if resp, errDo := client.Do(req); errDo != nil {
						fmt.Println(errDo.Error())
					} else {
						defer resp.Body.Close()
						respBody, errReadAllResponce := ioutil.ReadAll(resp.Body)
						if errReadAllResponce != nil {
							fmt.Println(errReadAllResponce.Error())
						}
						fmt.Println("main server says: ", string(respBody))
					}

				}
			}

		} else if strings.EqualFold(order, "Drop") {
			post, errPost := http.Post("http://"+server_addr+"/Drop", "application/octet-stream", bytes.NewBuffer([]byte(order)))
			if errPost != nil {
				fmt.Println(errPost)
			} else {
				respBody, errReadAllSendToServer := ioutil.ReadAll(post.Body)
				if errReadAllSendToServer != nil {
					fmt.Println(errReadAllSendToServer.Error())
				}

				msg := fmt.Sprintf("%s", respBody)
				fmt.Println("main server says: ", msg)
			}
		} else {
			fmt.Println("can not determined")
			break
		}

	}
}

func DataGetterHandler(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(request.Body)
		if errReadAll != nil {
			http.Error(writer, errReadAll.Error(), http.StatusBadRequest)
		}

		csv.NewReader(bytes.NewReader(body))
	}
}
