package main

import (
	"os"
	"log"
	"fmt"
	"bytes"
	"net/http"
	"encoding/csv"
	"encoding/json"
	"time"
)

//https://stackoverflow.com/questions/24455147/how-do-i-send-a-json-string-in-a-post-request-in-go
func PutRequest(jsonString string){
	const token = "<INSERT_AUTH_TOKEN>";

	const backfillURL = "http://localhost:3001/api/backfill/documents";
	var jsonStr = []byte(jsonString);
	req, _ := http.NewRequest("PUT", backfillURL, bytes.NewBuffer(jsonStr));
	req.Header.Set("Authorization", token);
	req.Header.Set("Content-Type", "application/json");

	client := &http.Client{};
	resp, err := client.Do(req);

	if err != nil {
			panic(err)
	}
	defer resp.Body.Close();

	fmt.Println("response Status:", resp.Status)
	// fmt.Println("response Headers:", resp.Header)
	// body, _ := ioutil.ReadAll(resp.Body)
	// fmt.Println("response Body:", string(body))
}

type snapshotAttributes struct {
	Id string	`json:"_id"`
	Competitor_id string	`json:"competitor_id"`
}

func parseRowsAndPUT(snapshotRows[][]string) {
	const limit = 100;
	var snapshots []snapshotAttributes = make([]snapshotAttributes, limit);

	for i, row := range snapshotRows {
		if(i==0){ // skip headers
			continue;
		}

		index := (i-1)%limit;
		snapshots[index] = snapshotAttributes{
			Id: row[0],
			Competitor_id: row[1],
		}

		if(i%limit==0 || i == len(snapshotRows)-1){
			fmt.Println(i);

			// generate json format
			data :=map[string] interface{} {
				"collection" : "charactersnapshots",
				"rows": snapshots,
			}
			jsonData,_ := json.Marshal(data);

			// send request
			PutRequest(string(jsonData));

			// reset snapshot array
			snapshots = nil;
			snapshots = make([]snapshotAttributes, limit);
		}		
	}

}


func main(){
	const file_name = "competitors_of_character_snapshots.csv"

	file, err := os.Open(file_name)
	if err != nil {
		log.Fatal(err)
	}

	csvReader := csv.NewReader(file);
	data, err := csvReader.ReadAll();

  if err != nil {
		log.Fatal(err);
	}
	defer file.Close();
	start := time.Now();

	parseRowsAndPUT(data);

	duration := time.Since(start);

	fmt.Println(duration);
}