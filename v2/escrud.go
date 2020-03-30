// Package escrud provides simple CRUD operations and search function to work with Elastic Search
package escrud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"io/ioutil"
	"log"
)

var Es *elasticsearch.Client

type Created struct {
	ID string `json:"_id"`
}

type Updated struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Result  string `json:"result"`
}

type Deleted struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Result  string `json:"result"`
}

type Got struct {
	Index   string      `json:"_index"`
	ID      string      `json:"_id"`
	Version int         `json:"_version"`
	Source  interface{} `json:"_source"`
}

func Connect(host string, port int) error {
	var err error

	//Es, err = elasticsearch.NewDefaultClient()
	esServer := fmt.Sprintf("http://%s:%d", host, port)
	cfg := elasticsearch.Config{Addresses: []string{esServer}}
	Es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s\n", err)
		return err
	}
	log.Println(elasticsearch.Version)
	info, err := Es.Info()
	if err != nil {
		log.Printf("Cannot get server info: %s\n", err)
		log.Printf("You should check Elastic health!")
		return err
	}

	log.Println(info)

	return nil
}

func Update(index, id string, data []byte) (Updated, error) {
	templ := []byte(`{"doc":`)
	templ = append(templ, data...)
	templ = append(templ, []byte(`}`)...)

	var upd Updated
	res, err := Es.Update(
		index,
		id,
		bytes.NewReader(templ),
		Es.Update.WithPretty(),
	)
	if err != nil {
		return upd, fmt.Errorf("cannot update entry: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return upd, fmt.Errorf("bad connection? Status: %s, err: %v", res.Status(), err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return upd, fmt.Errorf("cannot read response body: %v", err)
	}

	if err := json.Unmarshal(resp, &upd); err != nil {
		return upd, fmt.Errorf("response contains bad json: %v", err)
	}

	return upd, nil
}

// Exists checks if there's a document with such id in such an index
func Exists(index string, id string) (exists bool, err error) {
	if len(id) < 1 {
		return false, fmt.Errorf("id too short")
	}

	if len(index) < 1 {
		return false, fmt.Errorf("index name too short")
	}

	res, err := Es.Exists(index, id)
	if err != nil {
		return false, err
	}
	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("[%s]", res.Status())
	}
}

// Create should contain a valid JSON with key {..."id":your_unique_id}
func Create(index string, id string, data []byte) (err error) {
	if len(data) < 1 {
		data = []byte(fmt.Sprintf(`{"id":%s}`, id))
	}

	res, err := Es.Index(
		index,
		bytes.NewReader(data),
		Es.Index.WithDocumentID(id),
		Es.Index.WithPretty(),
	)
	if err != nil {
		return fmt.Errorf("cannot create entry: %v", err)
	}
	defer res.Body.Close()

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("cannot read response body: %v", err)
	}

	if res.IsError() {
		// с доков пакета го-эластик.
		//var e map[string]interface{}
		//if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		//	log.Fatalf("Error parsing the response body: %s", err)
		//} else {
		//	// Print the response status and error information.
		//	log.Fatalf("[%s] %s: %s",
		//		res.Status(),
		//		e["error"].(map[string]interface{})["type"],
		//		e["error"].(map[string]interface{})["reason"],
		//	)
		//}
		return fmt.Errorf("bad connection? %s", resp)
	}

	var created Created
	if err := json.Unmarshal(resp, &created); err != nil {
		return fmt.Errorf("response contains bad json: %v", err)
	}

	return nil
}

func Delete(index, id string) (Deleted, error) {
	var deleted Deleted
	res, err := Es.Delete(index, id,
		Es.Delete.WithPretty())
	if err != nil {
		return deleted, fmt.Errorf("cannot delete entry: %v", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return deleted, fmt.Errorf("bad connection? %v", err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return deleted, fmt.Errorf("cannot read response body: %v", err)
	}

	if err := json.Unmarshal(resp, &deleted); err != nil {
		return deleted, fmt.Errorf("response contains bad json: %v", err)
	}

	return deleted, nil
}

func Source(index, id string) ([]byte, error) {
	res, err := Es.GetSource(index, id,
		Es.GetSource.WithPretty())
	if err != nil {
		return nil, fmt.Errorf("cannot read entry: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("bad connection? %v", err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body: %v", err)
	}

	return resp, nil
}

func Read(index, id string) (Got, error) {
	var got Got
	res, err := Es.Get(index, id,
		//Es.Get.WithSourceIncludes("text,user"),
		Es.Get.WithPretty())
	if err != nil {
		return got, fmt.Errorf("cannot read entry: %v", err)
	}
	if res.IsError() {
		return got, fmt.Errorf("bad connection? %v", err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return got, fmt.Errorf("cannot read response body: %v", err)
	}

	if err := json.Unmarshal(resp, &got); err != nil {
		return got, fmt.Errorf("response contains bad json: %v", err)
	}

	return got, nil
}
