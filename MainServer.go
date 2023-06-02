package main

import (
	"bytes"
	"container/list"
	"fmt"
	"io/ioutil"
	"log"
	net "net"
	"net/http"
	"strings"
	"sync"
)

var mgLocalIP = "192.168.1.3:5566"
var wg sync.WaitGroup

func main() {
	//clients := list.New()
	mux := http.NewServeMux()
	//wg.Add(2)
	//OpenTcpMgServerConnection()
	//go OpenTcpClientConnection(clients)
	mux.HandleFunc("/uploadCsv", ClientUploadHandler)
	mux.HandleFunc("/getAllData", ClientGetDataRequest)
	mux.HandleFunc("/updateData", ClientUpdateDataRequest)
	mux.HandleFunc("/Info", ClientRetrieveInfoRequest)
	mux.HandleFunc("/Drop", ClientRetrieveDropRequest)

	log.Fatal(http.ListenAndServe(":8080", mux))

	//wg.Wait()

}

func ClientRetrieveDropRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}
		_, _ = fmt.Fprintf(w, "command received!")

		command := string(body)

		if resp, errNewRequest := http.Post("http://localhost:9080/Drop", "application/octet-stream", bytes.NewBuffer([]byte(command))); errNewRequest != nil {
			fmt.Println(errNewRequest.Error())
		} else {
			//defer resp.Body.Close()

			respBody, errReadAllSendToServer := ioutil.ReadAll(resp.Body)
			if errReadAllSendToServer != nil {
				fmt.Println(errReadAllSendToServer.Error())
			}
			fmt.Println("mango server says: ", string(respBody))

			fmt.Fprintf(w, "main server receives: ", string(respBody))
		}
	}
}

func ClientRetrieveInfoRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}
		_, _ = fmt.Fprintf(w, "command received!")

		command := string(body)

		if resp, errNewRequest := http.Post("http://localhost:9080/MgInfo", "application/octet-stream", bytes.NewBuffer([]byte(command))); errNewRequest != nil {
			fmt.Println(errNewRequest.Error())
		} else {
			//defer resp.Body.Close()

			respBody, errReadAllSendToServer := ioutil.ReadAll(resp.Body)
			if errReadAllSendToServer != nil {
				fmt.Println(errReadAllSendToServer.Error())
			}
			fmt.Println("mango server says: ", string(respBody))

			fmt.Fprintf(w, "main server receives: ", string(respBody))
		}
	}
}

func ClientUpdateDataRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		errParseMultipartForm := r.ParseMultipartForm(10 << 20)
		if errParseMultipartForm != nil {
			http.Error(w, errParseMultipartForm.Error(), http.StatusBadRequest)
		}

		file, _, errFormFile := r.FormFile("file")
		if errFormFile != nil {
			http.Error(w, errFormFile.Error(), http.StatusBadRequest)
		}

		reads, errReadAll := ioutil.ReadAll(file)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusInternalServerError)
		}

		fmt.Fprintf(w, "file received. Size %d bytes", len(reads))

		if resp, errNewRequest := http.Post("http://localhost:9080/MgServerUpdateCsv", "application/octet-stream", bytes.NewBuffer(reads)); errNewRequest != nil {
			fmt.Println(errNewRequest.Error())
		} else {
			defer resp.Body.Close()

			respBody, errReadAllSendToServer := ioutil.ReadAll(resp.Body)
			if errReadAllSendToServer != nil {
				fmt.Println(errReadAllSendToServer.Error())
			}
			fmt.Println("mango server says: ", string(respBody))
		}
	}
}

func OpenTcpMgServerConnection() {
	if conn, err := net.Listen("tcp", "7788"); err != nil {
		fmt.Println(err.Error())
	} else {
		for {
			if accept, err2 := conn.Accept(); err2 != nil {
				fmt.Println(err2.Error())
			} else {
				buffer := make([]byte, 1024)
				_, _ = accept.Read(buffer)
				msg := string(buffer)
				fmt.Println(msg)
			}
		}
	}
}

func ClientGetDataRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}
		_, _ = fmt.Fprintf(w, "command received!")

		command := string(body)

		if resp, errNewRequest := http.Post("http://localhost:9080/MgServerDataGetter", "application/octet-stream", bytes.NewBuffer([]byte(command))); errNewRequest != nil {
			fmt.Println(errNewRequest.Error())
		} else {
			//defer resp.Body.Close()

			respBody, errReadAllSendToServer := ioutil.ReadAll(resp.Body)
			if errReadAllSendToServer != nil {
				fmt.Println(errReadAllSendToServer.Error())
			}
			fmt.Println("mango server says: ", string(respBody))

			fmt.Fprintf(w, "file data", respBody)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func ClientUploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		errParseMultipartForm := r.ParseMultipartForm(10 << 20)
		if errParseMultipartForm != nil {
			http.Error(w, errParseMultipartForm.Error(), http.StatusBadRequest)
		}

		file, _, errFormFile := r.FormFile("file")
		if errFormFile != nil {
			http.Error(w, errFormFile.Error(), http.StatusBadRequest)
		}

		reads, errReadAll := ioutil.ReadAll(file)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusInternalServerError)
		}

		fmt.Fprintf(w, "file received. Size %d bytes", len(reads))

		if resp, errNewRequest := http.Post("http://localhost:9080/MgServerUploadCsv", "application/octet-stream", bytes.NewBuffer(reads)); errNewRequest != nil {
			fmt.Println(errNewRequest.Error())
		} else {
			defer resp.Body.Close()

			respBody, errReadAllSendToServer := ioutil.ReadAll(resp.Body)
			if errReadAllSendToServer != nil {
				fmt.Println(errReadAllSendToServer.Error())
			}
			fmt.Println("mango server says: ", string(respBody))
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

}

func OpenTcpClientConnection(clients *list.List) {
	listenerClientPort, errClientPort := net.Listen("tcp", ":8080")
	if errClientPort != nil {
		fmt.Println(errClientPort)
		return
	}

	for {
		accept, err := listenerClientPort.Accept()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(accept.LocalAddr())

		// Receive the string from the remote device
		buffer := make([]byte, 1024)
		n, err := accept.Read(buffer)
		if err != nil {
			fmt.Println(err)
			return
		}
		receivedMessage := string(buffer[:n])
		fmt.Println(receivedMessage)

		if strings.Contains(receivedMessage, "create") {

		} else if strings.EqualFold("get all data", receivedMessage) {
			connY, errY := net.Dial("tcp", mgLocalIP)
			if errY != nil {
				fmt.Println(errY)
				return
			}
			defer connY.Close()
			message := clients.Back().Value.(string)
			_, err = connY.Write([]byte(message))
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("order sent successfully to the chunk")
		} else if strings.Contains(strings.Split(receivedMessage, " ")[0], "put") {
			_, errY := net.Dial("tcp", mgLocalIP)
			if errY != nil {
				fmt.Println("line 136", errY)
				return
			}
			//defer connY.Close()

			fmt.Println("put command received")
			for {
				accept, err := listenerClientPort.Accept()
				if err != nil {
					log.Fatal(err)
				}

				byteFileBuf := make([]byte, 1024)
				fmt.Println("local ip send file: ", accept.LocalAddr())
				if n, errReadFile := accept.Read(byteFileBuf); errReadFile != nil {
					fmt.Println(errReadFile.Error())
					break
				} else {
					msg := string(byteFileBuf[:n])
					fmt.Println(msg)
				}

				break
				//
				//if _, errSent := connY.Write([]byte("create")); err != nil {
				//	fmt.Println(errSent.Error())
				//} else {
				//	continue
				//}
				//fmt.Println(fileContent)
				//if connY2, errY2 := net.Dial("tcp", slaves.Back().Value.(string)); errY2 != nil {
				//	fmt.Println(errY2.Error())
				//} else {
				//	_, _ = io.Copy(connY2, bytes.NewReader(fileContent))
				//	break
				//}
			}
			//message := clients.Back().Value.(string) + ",,,," + strings.Split(receivedMessage, " ")[1]
			//_, err = connY.Write([]byte(message))
			//if err != nil {
			//	fmt.Println(err)
			//	return
			//}
			fmt.Println("order sent successfully to the chunk")
		} else if strings.EqualFold(strings.Split(receivedMessage, ",")[1], "client") {
			clients.PushBack(strings.Split(receivedMessage, ":")[1] + ":" + strings.Split(strings.Split(receivedMessage, ":")[2], ",")[0])
		}

		if clients.Len() != 0 {
			fmt.Println("last client ip : ", clients.Back().Value)
		}
	}

	wg.Done()
}
