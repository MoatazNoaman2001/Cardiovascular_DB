package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var database mongo.Database
var ctx context.Context
var dbName string = "Vitalism_Cardiovascular_Burden_1"

func main() {
	mux := http.NewServeMux()
	//startMGServices()
	mux.HandleFunc("/MgServerUploadCsv", MainServerHandleRecievedData)
	mux.HandleFunc("/MgServerDataGetter", MainServerHandleDataSender)
	mux.HandleFunc("/MgServerUpdateCsv", MainServerHandleUpdate)
	mux.HandleFunc("/MgInfo", MainServerHandleInfo)
	mux.HandleFunc("/Drop", MgServerHandleDrop)

	log.Fatal(http.ListenAndServe(":9080", mux))
	//ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	//res := getAllInDb(ctx)
	//fmt.Println(res.Next(ctx))
}

func MgServerHandleDrop(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}

		fmt.Println("Order", string(body))

		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
		if err != nil {
			log.Fatal(err)
		}
		err = client.Connect(ctx)
		if err != nil {
			log.Fatal(err)
		}

		client.Database(dbName).Drop(ctx)
		fmt.Fprintf(w, "reposnce ", "done")
	}
}

func MainServerHandleInfo(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}

		fmt.Println("Order", string(body))

		msg := connectToMgServer(dbName)
		fmt.Fprintf(w, "reposnce ", msg)
	}
}

func MainServerHandleUpdate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}
		fmt.Fprintf(w, "Bytes received!")

		fmt.Println(string(body))

		reader := csv.NewReader(bytes.NewReader(body))

		ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
		client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
		if err != nil {
			log.Fatal(err)
		}
		err = client.Connect(ctx)
		if err != nil {
			log.Fatal(err)
		}

		var records []interface{}
		headers, _ := reader.Read()
		fmt.Println("headers", headers, "\n")
		rows, errReadAll := reader.ReadAll()
		if errReadAll != nil {
			fmt.Println(errReadAll.Error())
		}

		for _, row := range rows[1:] {
			rec := bson.D{}
			for i, s := range row {
				rec = append(rec, bson.E{Key: headers[i], Value: s})
			}
			records = append(records, rec)
		}

		UpadateInDb(ctx, records, client)

		fmt.Fprintf(w, "update is succsseded")
	}
}

func MainServerHandleRecievedData(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}
		fmt.Fprintf(w, "Bytes received!")

		reader := csv.NewReader(bytes.NewReader(body))
		var records []interface{}
		headers, _ := reader.Read()
		fmt.Println("headers", headers, "\n")

		rows, _ := reader.ReadAll()
		fmt.Println(headers)
		fmt.Println(rows[:5])
		for _, row := range rows[1:] {
			rec := bson.D{}
			for i, s := range row {
				rec = append(rec, bson.E{Key: headers[i], Value: s})
			}
			records = append(records, rec)
		}

		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		InsertInDb(ctx, records)
		fmt.Println(len(records))
		for _, record := range records[:5] {
			fmt.Println(record)
		}
		//fmt.Println("interface  elements : ", records[:5])
	}
}
func MainServerHandleDataSender(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, errReadAll := ioutil.ReadAll(r.Body)
		if errReadAll != nil {
			http.Error(w, errReadAll.Error(), http.StatusBadRequest)
		}
		fmt.Fprintf(w, "command recieved", strings.SplitN(string(body), "=", 1))

		command := strings.Split(string(body), " ")
		fmt.Println(string(body))
		if strings.HasPrefix(command[0], "get") {
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
			if err != nil {
				log.Fatal(err)
			}
			err = client.Connect(ctx)
			if err != nil {
				log.Fatal(err)
			}
			if len(command) == 2 && !strings.HasPrefix(command[1], "-") {
				entries := findCollectionByFilter(ctx, client.Database(dbName), command[1], []interface{}{})

				buf := new(bytes.Buffer)
				CsvWriter := csv.NewWriter(buf)
				for _, entry := range entries {
					var row []string
					for s, _ := range entry {
						row = append(row, s)
					}
					CsvWriter.Write(row)
					break
				}
				for _, entry := range entries {
					var row []string
					for _, i2 := range entry {
						switch i2.(type) {
						case string:
							row = append(row, i2.(string))
						case primitive.ObjectID:
							row = append(row, i2.(primitive.ObjectID).String())
						}
					}

					CsvWriter.Write(row)
				}

				CsvWriter.Flush()

				csv_bytes := make([]byte, 1024*1024*24)
				n, errReadCsvBytes := buf.Read(csv_bytes)
				if errReadCsvBytes != nil {
					fmt.Println(errReadCsvBytes.Error())
				}
				fmt.Println(n)
				dial, errg := net.Dial("tcp", "192.168.1.3:9090")
				if errg != nil {
					fmt.Println(errg.Error())
				}
				_, errw := dial.Write(csv_bytes[:n])
				if errw != nil {
					fmt.Println(errw.Error())
				}

			} else if len(command) > 2 {
				var collectionName string
				optionsArr := make([]string, len(command)-2)
				for _, s := range command {
					if !strings.HasPrefix(s, "-") {
						collectionName = s
					} else {
						optionsArr = append(optionsArr, s)
					}
				}

				var inter []interface{}
				for _, s := range optionsArr {
					if strings.HasPrefix(s, "-filter") {
						parms_arr := strings.Split(strings.Split(s, "\"")[1], ";")
						filterMatch := bson.D{}
						for _, parm := range parms_arr {
							if strings.Contains(parm, ">") {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, ">")[0], bson.D{{"$gt", strings.Split(parm, ">")[1]}}})
							} else if strings.Contains(parm, ">=") {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, ">=")[0], bson.D{{"$gte", strings.Split(parm, ">=")[1]}}})
							} else if strings.Contains(parm, "<") {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, "<")[0], bson.D{{"$lt", strings.Split(parm, "<")[1]}}})
							} else if strings.Contains(parm, "<=") {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, "<=")[0], bson.D{{"$lte", strings.Split(parm, "<=")[1]}}})
							} else if strings.Contains(parm, "=") {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, "=")[0], strings.Split(parm, "=")[1]})
							}
						}
						inter = append(inter, bson.D{{"$match", filterMatch}})
					} else if strings.HasPrefix(s, "-sort") {
						parms_arr := strings.Split(strings.Split(s, "\"")[1], ";")
						filterMatch := bson.D{}
						for _, parm := range parms_arr {
							if strings.Split(parm, ":")[1] == "1" {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, ":")[0], 1})
							} else {
								filterMatch = append(filterMatch, bson.E{strings.Split(parm, ":")[0], -1})
							}
						}
						inter = append(inter, bson.D{{"$sort", filterMatch}})
					} else if strings.HasPrefix(s, "-groupBy") {
						parms_arr := strings.Split(strings.Split(s, "\"")[1], ";")
						if len(parms_arr) != 4 {
							fmt.Fprintf(w, "groupBy order should have 4 inputs (form , localField , foreignField , as)")
							continue
						}
						filterMatch := bson.D{}
						for _, parm := range parms_arr {
							filterMatch = append(filterMatch, bson.E{strings.Split(parm, ":")[0], strings.Split(parm, ":")[1]})
						}
						inter = append(inter, bson.D{{"$lookup", filterMatch}})
					} else if strings.HasPrefix(s, "-project") {
						parms_arr := strings.Split(strings.Split(s, "\"")[1], ";")
						filterMatch := bson.D{}
						for _, parm := range parms_arr {
							filterMatch = append(filterMatch, bson.E{strings.Split(parm, ":")[0], strings.Split(parm, ":")[1]})
						}
						inter = append(inter, bson.D{{"$project", filterMatch}})
					} else if strings.HasPrefix(s, "-addField") {
						parms_arr := strings.Split(strings.Split(s, "\"")[1], ";")
						addField := bson.D{}
						for _, parm := range parms_arr {
							addField = append(addField, bson.E{strings.Split(parm, ":")[0], bson.D{{strings.Split(parm, ":")[1], strings.Split(parm, ":")[2]}}})
						}
						inter = append(inter, bson.D{{"$project", addField}})
					}
				}
				fmt.Println(inter)
				entries := findCollectionByFilter(ctx, client.Database(dbName), collectionName, inter)

				buf := new(bytes.Buffer)
				CsvWriter := csv.NewWriter(buf)
				for _, entry := range entries {
					var row []string
					for s, _ := range entry {
						row = append(row, s)
					}
					CsvWriter.Write(row)
					break
				}
				for _, entry := range entries {
					var row []string
					for _, i2 := range entry {
						switch i2.(type) {
						case string:
							row = append(row, i2.(string))
						case primitive.ObjectID:
							row = append(row, i2.(primitive.ObjectID).String())
						}
					}

					CsvWriter.Write(row)
				}

				CsvWriter.Flush()

				csv_bytes := make([]byte, 1024*1024*24)
				n, errReadCsvBytes := buf.Read(csv_bytes)
				if errReadCsvBytes != nil {
					fmt.Println(errReadCsvBytes.Error())
				}
				fmt.Println(n)
				dial, errg := net.Dial("tcp", "192.168.1.3:9090")
				if errg != nil {
					fmt.Println(errg.Error())
				}
				_, errw := dial.Write(csv_bytes[:n])
				if errw != nil {
					fmt.Println(errw.Error())
				}
			}
			// Set the content type and headers
			w.Header().Set("Content-Type", "text/csv")
			w.Header().Set("Content-Disposition", "attachment; filename=data.csv")
		}
		//fmt.Println("interface  elements : ", records[:5])
	}
}

func getAllInDb(ctx2 context.Context) *mongo.Cursor {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	diseaseCol := client.Database(dbName).Collection("disease")
	//regionsCol := client.Database(dbName).Collection("regions")
	//locationsCol := client.Database(dbName).Collection("locations")
	//sexCol := client.Database(dbName).Collection("sex")
	//yearsCol := client.Database(dbName).Collection("years")
	//ageGroupsCol := client.Database(dbName).Collection("age_groups")
	//measureMetricNamesCol := client.Database(dbName).Collection("measure_metric_names")
	//_ = client.Database(dbName).Collection("rei")
	//valuesCol := client.Database(dbName).Collection("values")

	aggregate, err := diseaseCol.Aggregate(ctx, []interface{}{
		bson.D{{"$lookup", bson.D{{"from", "rei"}, {"localField", "cause_id"}, {"foreignField", "cause_id"}, {"as", "rei_related"}}}},
		bson.D{{"$lookup", bson.D{{"from", "age_groups"}, {"localField", "cause_id"}, {"foreignField", "cause_id"}, {"as", "age_groups_related"}}}},
		bson.D{{"$lookup", bson.D{{"from", "sex"}, {"localField", "cause_id"}, {"foreignField", "cause_id"}, {"as", "sex_related"}}}},
		bson.D{{"$lookup", bson.D{{"from", "values"}, {"localField", "cause_id"}, {"foreignField", "cause_id"}, {"as", "values_related"}}}},
		bson.D{{"$lookup", bson.D{{"from", "years"}, {"localField", "cause_id"}, {"foreignField", "cause_id"}, {"as", "years_related"}}}},
		bson.D{{"$lookup", bson.D{{"from", "locations"}, {"localField", "cause_id"}, {"foreignField", "cause_id"}, {"as", "locations_related"}}}},
		bson.D{{"$unwind", "$rei_related"}},
		bson.D{{"$unwind", "$age_groups_related"}},
		bson.D{{"$unwind", "$sex_related"}},
		bson.D{{"$unwind", "$values_related"}},
		bson.D{{"$unwind", "$years_related"}},
		bson.D{{"$unwind", "$locations_related"}},
	})
	if err != nil {
		return nil
	}
	return aggregate
}

func UpadateInDb(ctx context.Context, records []interface{}, client *mongo.Client) {
	diseaseCol := client.Database(dbName).Collection("disease")
	regionsCol := client.Database(dbName).Collection("regions")
	locationsCol := client.Database(dbName).Collection("locations")
	sexCol := client.Database(dbName).Collection("sex")
	yearsCol := client.Database(dbName).Collection("years")
	ageGroupsCol := client.Database(dbName).Collection("age_groups")
	measureMetricNamesCol := client.Database(dbName).Collection("measure_metric_names")
	_ = client.Database(dbName).Collection("rei")
	valuesCol := client.Database(dbName).Collection("values")

	for _, record := range records {
		_, errDiseaseManyUpdate := diseaseCol.UpdateOne(ctx, bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.E{Key: "cause_name", Value: record.(bson.D).Map()["cause_name"]}}})
		if errDiseaseManyUpdate != nil {
			fmt.Println(errDiseaseManyUpdate.Error())
		}
	}

	for _, record := range records {
		_, errRegionsManyUpdate := regionsCol.UpdateOne(ctx, bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{{"region_id", record.(bson.D).Map()["region_id"]}, {"region_name", record.(bson.D).Map()["region_name"]}}}})
		if errRegionsManyUpdate != nil {
			fmt.Println(errRegionsManyUpdate.Error())
		}

	}

	for _, record := range records {
		_, errLocationManyUpdate := locationsCol.UpdateOne(ctx,
			bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{{"location_id", record.(bson.D).Map()["location_id"]},
				{"location_name", record.(bson.D).Map()["location_name"]}}}},
		)
		if errLocationManyUpdate != nil {
			fmt.Println(errLocationManyUpdate.Error())
		}
	}

	for _, record := range records {
		_, errSexManyUpdate := sexCol.UpdateOne(ctx,
			bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{{"sex_id", record.(bson.D).Map()["sex_id"]},
				{"sex", record.(bson.D).Map()["sex"]}}}},
		)
		if errSexManyUpdate != nil {
			fmt.Println(errSexManyUpdate.Error())
		}
	}

	for _, record := range records {

		_, errYearManyUpdate := yearsCol.UpdateOne(ctx,
			bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{{"year_id", record.(bson.D).Map()["year_id"]}}}})
		if errYearManyUpdate != nil {
			fmt.Println(errYearManyUpdate.Error())
		}
	}

	for _, record := range records {
		_, errAgeGroupManyUpdate := ageGroupsCol.UpdateOne(ctx,
			bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{{"age_group_id", record.(bson.D).Map()["age_group_id"]},
				{"age_group_name", record.(bson.D).Map()["age_group_name"]}}}})
		if errAgeGroupManyUpdate != nil {
			fmt.Println(errAgeGroupManyUpdate.Error())
		}
	}

	for _, record := range records {
		_, errmetric_measure_namesManyUpdate := measureMetricNamesCol.UpdateOne(ctx,
			bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{{"measure_name", record.(bson.D).Map()["measure_name"]},
				{"metric_name", record.(bson.D).Map()["metric_name"]}}}})
		if errmetric_measure_namesManyUpdate != nil {
			fmt.Println(errmetric_measure_namesManyUpdate.Error())
		}
	}

	for _, record := range records {
		_, errValuesManyInsert := valuesCol.UpdateOne(ctx,
			bson.D{{"cause_id", record.(bson.D).Map()["cause_id"]}},
			bson.D{{"$set", bson.D{
				{"val", record.(bson.D).Map()["val"]},
				{"upper", record.(bson.D).Map()["upper"]},
				{"lower", record.(bson.D).Map()["lower"]},
			}}})
		if errValuesManyInsert != nil {
			fmt.Println(errValuesManyInsert.Error())
		}
	}
}

func InsertInDb(ctx2 context.Context, records []interface{}) {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	diseaseCol := client.Database(dbName).Collection("disease")
	regionsCol := client.Database(dbName).Collection("regions")
	locationsCol := client.Database(dbName).Collection("locations")
	sexCol := client.Database(dbName).Collection("sex")
	yearsCol := client.Database(dbName).Collection("years")
	ageGroupsCol := client.Database(dbName).Collection("age_groups")
	measureMetricNamesCol := client.Database(dbName).Collection("measure_metric_names")
	_ = client.Database(dbName).Collection("rei")
	valuesCol := client.Database(dbName).Collection("values")

	var diseases []interface{}
	for _, record := range records {
		diseases = append(diseases,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"cause_name", record.(bson.D).Map()["cause_name"]},
			})
	}
	_, errDiseaseManyInsert := diseaseCol.InsertMany(ctx, diseases)
	if errDiseaseManyInsert != nil {
		fmt.Println(errDiseaseManyInsert.Error())
	}

	var regions []interface{}
	for _, record := range records {
		regions = append(regions,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"region_id", record.(bson.D).Map()["region_id"]},
				{"region_name", record.(bson.D).Map()["region_name"]},
			})
	}
	_, errRegionsManyInsert := regionsCol.InsertMany(ctx, regions)
	if errRegionsManyInsert != nil {
		fmt.Println(errRegionsManyInsert.Error())
	}

	var locations []interface{}
	for _, record := range records {
		locations = append(locations,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"location_id", record.(bson.D).Map()["location_id"]},
				{"location_name", record.(bson.D).Map()["location_name"]},
			})
	}
	_, errLocationManyInsert := locationsCol.InsertMany(ctx, locations)
	if errLocationManyInsert != nil {
		fmt.Println(errLocationManyInsert.Error())
	}

	var sexs []interface{}
	for _, record := range records {
		sexs = append(sexs,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"sex_id", record.(bson.D).Map()["sex_id"]},
				{"sex", record.(bson.D).Map()["sex"]},
			})
	}
	_, errSexManyInsert := sexCol.InsertMany(ctx, sexs)
	if errSexManyInsert != nil {
		fmt.Println(errSexManyInsert.Error())
	}

	var year []interface{}
	for _, record := range records {
		year = append(year,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"year_id", record.(bson.D).Map()["year_id"]},
			})
	}
	_, errYearManyInsert := yearsCol.InsertMany(ctx, year)
	if errYearManyInsert != nil {
		fmt.Println(errYearManyInsert.Error())
	}

	var age_groups []interface{}
	for _, record := range records {
		age_groups = append(age_groups,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"age_group_id", record.(bson.D).Map()["age_group_id"]},
				{"age_group_name", record.(bson.D).Map()["age_group_name"]},
			})
	}
	_, errAgeGroupManyInsert := ageGroupsCol.InsertMany(ctx, age_groups)
	if errAgeGroupManyInsert != nil {
		fmt.Println(errAgeGroupManyInsert.Error())
	}

	var metric_measure_names []interface{}
	for _, record := range records {
		metric_measure_names = append(metric_measure_names,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"measure_name", record.(bson.D).Map()["measure_name"]},
				{"metric_name", record.(bson.D).Map()["metric_name"]},
			})
	}
	_, errmetric_measure_namesManyInsert := measureMetricNamesCol.InsertMany(ctx, metric_measure_names)
	if errmetric_measure_namesManyInsert != nil {
		fmt.Println(errmetric_measure_namesManyInsert.Error())
	}

	//var reis []interface{}
	//for _, record := range records {
	//	reis = append(reis,
	//		bson.D{
	//			{"cause_id", record.(bson.D).Map()["cause_id"]},
	//			{"rei", record.(bson.D).Map()["rei"]},
	//			{"rei_name", record.(bson.D).Map()["rei_name"]},
	//		})
	//}
	//_, errreisManyInsert := reiCol.InsertMany(ctx, reis)
	//if errreisManyInsert != nil {
	//	fmt.Println(errreisManyInsert.Error())
	//}

	var values []interface{}
	for _, record := range records {
		values = append(values,
			bson.D{
				{"cause_id", record.(bson.D).Map()["cause_id"]},
				{"val", record.(bson.D).Map()["val"]},
				{"upper", record.(bson.D).Map()["upper"]},
				{"lower", record.(bson.D).Map()["lower"]},
			})
	}
	_, errValuesManyInsert := valuesCol.InsertMany(ctx, values)
	if errValuesManyInsert != nil {
		fmt.Println(errValuesManyInsert.Error())
	}

}

func startMGServices() {
	listener, err := net.Listen("tcp", ":1080")
	if err != nil {
		return
	}

	is_init := false
	for {
		accept, err := listener.Accept()
		if err != nil {
			return
		}

		fmt.Println(accept.LocalAddr())
		buffer := make([]byte, 1024)
		read, err := accept.Read(buffer)
		if err != nil {
			return
		}
		receiveMessage := string(buffer[:read])
		fmt.Println(receiveMessage)

		if strings.EqualFold(receiveMessage, "create") {
			for {
				accept, err := listener.Accept()
				if err != nil {
					log.Fatal(err)
				}

				byteFileBuf := make([]byte, 1024)
				fmt.Println(accept.LocalAddr())
				n, errReadFile := io.ReadFull(accept, byteFileBuf)
				if errReadFile != nil {
					fmt.Println(errReadFile.Error())
					return
				}
				fileContent := string(buffer[:n])
				println(fileContent)

			}
		} else {
			if !is_init {
				connectToMgServer(receiveMessage)
				is_init = !is_init
			}
		}

	}
}

func connectToMgServer(DbName string) string {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	//defer client.Disconnect(ctx)

	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases[0])
	is_contain := false
	for _, database := range databases {
		if strings.EqualFold(DbName, database) {
			is_contain = !is_contain
			break
		}
	}
	if is_contain {
		diseaseCol := client.Database(DbName).Collection("disease")
		regionsCol := client.Database(DbName).Collection("regions")
		locationsCol := client.Database(DbName).Collection("locations")
		sexCol := client.Database(DbName).Collection("sex")
		yearsCol := client.Database(DbName).Collection("years")
		ageGroupsCol := client.Database(DbName).Collection("age_groups")
		measureMetricNamesCol := client.Database(DbName).Collection("measure_metric_names")
		_ = client.Database(DbName).Collection("rei")
		valuesCol := client.Database(DbName).Collection("values")

		diseaseCount, _ := diseaseCol.CountDocuments(ctx, bson.D{})
		regionCount, _ := regionsCol.CountDocuments(ctx, bson.D{})
		locationCount, _ := locationsCol.CountDocuments(ctx, bson.D{})
		sexCount, _ := sexCol.CountDocuments(ctx, bson.D{})
		yearsCount, _ := yearsCol.CountDocuments(ctx, bson.D{})
		ageGroupCount, _ := ageGroupsCol.CountDocuments(ctx, bson.D{})
		measureMetricCount, _ := measureMetricNamesCol.CountDocuments(ctx, bson.D{})
		valueCount, _ := valuesCol.CountDocuments(ctx, bson.D{})

		msg := "DataBase: " + DbName + "\tCollections name and size there :\n" +
			"Disease cols count : " + strconv.Itoa(int(diseaseCount)) + "\n" +
			"Region cols count : " + strconv.Itoa(int(regionCount)) + "\n" +
			"Location cols count : " + strconv.Itoa(int(locationCount)) + "\n" +
			"Sex cols count : " + strconv.Itoa(int(sexCount)) + "\n" +
			"Years cols count : " + strconv.Itoa(int(yearsCount)) + "\n" +
			"Age group cols count : " + strconv.Itoa(int(ageGroupCount)) + "\n" +
			"Measure Metric cols count : " + strconv.Itoa(int(measureMetricCount)) + "\n" +
			"Values cols count : " + strconv.Itoa(int(valueCount))

		return msg

		//findCollectionByFilter(ctx, database, "disease", []interface{}{})
	} else {

		return "no active data base found"
		//database := client.Database(DbName)
		//fmt.Println(database)
	}

	return ""
}

func SimpleDatabaseInsert(ctx context.Context, database mongo.Database, inter []interface{}) *mongo.InsertManyResult {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	insertManyResult, errInsertManu := client.Database("Vitalism_Cardiovascular_Burden_1").Collection("bigTable").InsertMany(ctx, inter)
	if errInsertManu != nil {
		fmt.Println(errInsertManu.Error())
	}

	return insertManyResult
}

func countRegionOccurrence(ctx2 context.Context) {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/"))
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(ctx2)
	if err != nil {
		log.Fatal(err)
	}

	_, _ = client.Database(dbName).Collection("bigTable").Distinct(ctx, "location_id", []interface{}{})

	client.Database(dbName).Collection("bigTable").Aggregate(ctx2, []interface{}{})
}

func updateOneDiseaseById(ctx context.Context, db mongo.Database, old_cause_id int, cause_id int, cause_name string) {
	res := db.Collection("disease").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}, {"cause_name", cause_name}}},
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
	db.Collection("locations").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("rei").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("region").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("sex").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("measure_metric_names").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("values").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("years").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("age_groups").FindOneAndUpdate(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
}
func updateAllDiseaseById(ctx context.Context, db mongo.Database, old_cause_id int, cause_id int, cause_name string) {
	_, err := db.Collection("disease").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}, {"cause_name", cause_name}}},
	})
	if err != nil {
		return
	}

	db.Collection("locations").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("rei").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("region").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("sex").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("measure_metric_names").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("values").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("years").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
	db.Collection("age_groups").UpdateMany(ctx, bson.D{{"cause_id", old_cause_id}}, bson.D{
		{"$set", bson.D{{"cause_id", cause_id}}},
	})
}

func updateLocationByCauseId(ctx context.Context, db mongo.Database, cause_id int, location_id int, location_name string) {
	res := db.Collection("locations").FindOneAndUpdate(ctx, bson.D{{"cause_id", cause_id}}, bson.D{
		{"$set", bson.D{{"location_id", location_id}, {"location_name", location_name}}},
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
}
func updateLocationByLocId(ctx context.Context, db mongo.Database, location_id int, location_name string) {
	res := db.Collection("locations").FindOneAndUpdate(ctx, bson.D{{"location_id", location_id}}, bson.D{
		{"$set", bson.D{{"cause_id", location_id}, {"location_name", location_name}}},
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
}
func updateReiByCauseId(ctx context.Context, db mongo.Database, cause_id int, rei int, rei_name string) {
	res := db.Collection("locations").FindOneAndUpdate(ctx, bson.D{{"cause_id", cause_id}}, bson.D{
		{"$set", bson.D{{"rei", rei}, {"rei_name", rei_name}}},
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
}
func updateValuesByCauseId(ctx context.Context, db mongo.Database, cause_id int, val float32, upper float32, lower float32) {
	res := db.Collection("locations").FindOneAndUpdate(ctx, bson.D{{"cause_id", cause_id}}, bson.D{
		{"$set", bson.D{{"val", val}, {"upper", upper}, {"lower", lower}}},
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
}
func updateMeasureAndMetricNameByCauseId(ctx context.Context, db mongo.Database, cause_id int, measure_name string, metric_name string) {
	res := db.Collection("locations").FindOneAndUpdate(ctx, bson.D{{"cause_id", cause_id}}, bson.D{
		{"$set", bson.D{{"measure_name", measure_name}, {"metric_name", metric_name}}},
	})
	if res.Err() != nil {
		log.Fatal(res.Err())
	}
}

func deleteDiseaseById(ctx context.Context, db mongo.Database, cause_id int) {
	db.Collection("disease").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("locations").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("rei").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("region").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("sex").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("measure_metric_names").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("values").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("years").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
	db.Collection("age_groups").FindOneAndDelete(ctx, []interface{}{bson.D{{"cause_id", cause_id}}})
}

func distinctDiseaseByCauseId(ctx context.Context, client *mongo.Database) {
	distinct, err := client.Collection("disease").Distinct(ctx, "cause_id", bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(distinct)
}
func findCollectionByFilter(ctx context.Context, client *mongo.Database, collectName string, dd []interface{}) []bson.M {
	if !reflect.DeepEqual(dd, []interface{}{}) {
		distinct, err := client.Collection(collectName).Aggregate(ctx, dd)
		if err != nil {
			fmt.Println(err.Error())
		}
		var entries []bson.M
		err2 := distinct.All(ctx, &entries)
		if err2 != nil {
			fmt.Println(err2.Error())
		}
		//fmt.Println(entries)
		return entries
	} else {
		distinct, err := client.Collection(collectName).Find(ctx, bson.D{})
		if err != nil {
			fmt.Println(err.Error())
		}
		var entries []bson.M
		err2 := distinct.All(ctx, &entries)
		if err2 != nil {
			fmt.Println(err2.Error())
		}
		//fmt.Println(entries)
		return entries
	}

}

func groupingDisease(ctx context.Context, client mongo.Client, db string) {
	entries := findCollectionByFilter(ctx, client.Database(db), "location", []interface{}{
		bson.D{
			{"$match", bson.D{{"cause_id", 792}}},
		},
		bson.D{
			{"$limit", 1000},
		},
		bson.D{
			{"$bucket", bson.D{
				{"groupBy", "$cause_id"},
				{"boundaries", []int{600, 650, 680, 690, 700, 750, 800}},
				{"default", "Others"},
				{"output", bson.D{
					{"count of occurrence", bson.D{{"$sum", 1}}},
					{"results", bson.D{
						{"$push", bson.D{
							{"id", "$cause_id"},
							{"name", "$cause_name"},
						}},
					}},
				}},
			}},
		},
	})
	for i, entry := range entries {
		fmt.Println(i, ":   ", entry)
	}
	fmt.Println(entries)
}

func ReiCounterRelatedToEachDisease(ctx context.Context, database *mongo.Database) {
	entries := findCollectionByFilter(ctx, database, "disease", []interface{}{
		bson.D{
			{"$lookup",
				bson.D{
					{"from", "rei"},
					{"localField", "cause_id"},
					{"foreignField", "cause_id"},
					{"as", "rei_related"}},
			},
		},
		bson.D{
			{"$addFields", bson.D{{"rei total count", bson.D{{"$size", "$rei_related"}}}}},
		},
		bson.D{
			{"$project", bson.D{
				{"cause_id", 1},
				{"cause_name", 1},
				{"rei_related", "$rei_related.rei"},
				{"rei total count", 1},
			}},
		},
	})

	fmt.Println(entries)
}

func insertRecord(ctx context.Context, db *mongo.Database,
	cause_id int, cause_name string,
	location_id int, location_name string,
	region_id int, region_name string,
	year_id int,
	rei_name string, rei string,
	sex_id int, sex string,
	age_group_id int, age_group_name string,
	metric_name string, measure_name string,
	val float32, upper float32, lower float32,
) {

	db.Collection("disease").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"cause_name", cause_name},
	})

	db.Collection("locations").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"location_id", location_id},
		{"location_name", location_name},
	})
	db.Collection("regions").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"region_id", region_id},
		{"region_name", region_name},
	})
	db.Collection("sex").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"sex_id", sex_id},
		{"sex", sex},
	})
	db.Collection("years").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"year", year_id},
	})
	db.Collection("age_groups").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"age_group_id", age_group_id},
		{"age_group_name", age_group_name},
	})
	db.Collection("measure_metric_names").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"metric_name", metric_name},
		{"measure_name", measure_name},
	})

	db.Collection("rei").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"rei_name", rei_name},
		{"rei", rei},
	})
	db.Collection("values").InsertOne(ctx, bson.D{
		{"cause_id", cause_id},
		{"val", val},
		{"lower", lower},
		{"upper", upper},
	})
}
