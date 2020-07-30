// Package escrud provides simple CRUD operations and search function to work with Elastic Search
package escrud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// ResponseBody struct from elastic
type ResponseBody struct {
	Index   string      `json:"_index,omitempty"`
	ID      string      `json:"_id,omitempty"`
	Version int         `json:"_version,omitempty"`
	Source  interface{} `json:"_source,omitempty"`
	Result  string      `json:"result,omitempty"`
}

// Client elasticsearch
type Client struct {
	Client *elasticsearch.Client
	Info   *esapi.Response
}

// Connect to a elastic
func Connect(host string, port int) (*Client, error) {
	var err error
	//Es, err = elasticsearch.NewDefaultClient()
	esServer := fmt.Sprintf("http://%s:%d", host, port)
	cfg := elasticsearch.Config{Addresses: []string{esServer}}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s\n", err)
		return nil, err
	}
	log.Println("Elasticsearch version:", elasticsearch.Version)
	info, err := es.Info()
	if err != nil {
		log.Printf("Cannot get server info: %s\n", err)
		log.Printf("You should check Elastic health!")
		return nil, err
	}
	return &Client{
		Client: es,
		Info:   info,
	}, nil
}

// Update record by id in elasticsearch
func (Es *Client) Update(index, id string, data []byte) (*ResponseBody, error) {
	return update(Es.Client, index, id, data)
}

// Exists checks if there's a document with such id in such an index
func (Es *Client) Exists(index string, id string) (bool, error) {
	return exists(Es.Client, index, id)
}

// Create record in elasticsearch
// should contain a valid JSON with key {..."id":your_unique_id}
func (Es *Client) Create(index string, id string, data []byte) error {
	return create(Es.Client, index, id, data)
}

// Delete record by id in elasticsearch
func (Es *Client) Delete(index, id string) (*ResponseBody, error) {
	return remove(Es.Client, index, id)
}

// Source get source
func (Es *Client) Source(index, id string) ([]byte, error) {
	return source(Es.Client, index, id)
}

func (Es *Client) Read(index, id string) (*ResponseBody, error) {
	return read(Es.Client, index, id)
}

type Updated struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Result  string `json:"result"`
}

// RemoveArrayItem удалить элемент массива по его параметру (пока только числовой ID)
// POST http://localhost:9200/mask/_update/_3/
// { "script": { "source": "ctx._source.mask_articles.removeIf(li -> li.article_id == params.article_id)", "params": { "article_id": 1886746 } } }
func (Es *Client) RemoveArrayItem(index string, docID string, arrayName string, itemName string, itemValue int) (Updated, error) {
	templ := fmt.Sprintf(`
{
  "script": {
    "source": "ctx._source.%s.removeIf(li -> li.%s == params.id)",
    "params": {
      "id": %d
    }
  }
}
`, arrayName, itemName, itemValue)

	var upd Updated
	res, err := Es.Client.Update(
		index,
		docID,
		bytes.NewReader([]byte(templ)),
		Es.Client.Update.WithPretty(),
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

func update(es *elasticsearch.Client, index, id string, data []byte) (*ResponseBody, error) {
	templ := []byte(`{"doc":`)
	templ = append(templ, data...)
	templ = append(templ, []byte(`}`)...)

	res, err := es.Update(
		index,
		id,
		bytes.NewReader(templ),
		es.Update.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot update entry: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("bad connection: status: %s, err: %v", res.Status(), err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body: %v", err)
	}

	var rb *ResponseBody
	if err := json.Unmarshal(resp, &rb); err != nil {
		return rb, fmt.Errorf("response contains bad json: %v", err)
	}
	return rb, nil
}

// Exists checks if there's a document with such id in such an index
func exists(es *elasticsearch.Client, index string, id string) (exists bool, err error) {
	if len(id) < 1 {
		return false, fmt.Errorf("id too short")
	}

	if len(index) < 1 {
		return false, fmt.Errorf("index name too short")
	}

	res, err := es.Exists(index, id)
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

func create(es *elasticsearch.Client, index string, id string, data []byte) (err error) {
	if len(data) < 1 {
		data = []byte(fmt.Sprintf(`{"id":%s}`, id))
	}

	res, err := es.Index(
		index,
		bytes.NewReader(data),
		es.Index.WithDocumentID(id),
		es.Index.WithPretty(),
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
		return fmt.Errorf("bad connection: %s", resp)
	}

	var rb ResponseBody
	if err := json.Unmarshal(resp, &rb); err != nil {
		return fmt.Errorf("response contains bad json: %v", err)
	}
	return nil
}

func remove(es *elasticsearch.Client, index, id string) (*ResponseBody, error) {
	res, err := es.Delete(index, id,
		es.Delete.WithPretty())
	if err != nil {
		return nil, fmt.Errorf("cannot delete entry: %v", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("bad connection: %v", err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body: %v", err)
	}

	var rb *ResponseBody
	if err := json.Unmarshal(resp, &rb); err != nil {
		return rb, fmt.Errorf("response contains bad json: %v", err)
	}
	return rb, nil
}

func source(es *elasticsearch.Client, index, id string) ([]byte, error) {
	res, err := es.GetSource(index, id,
		es.GetSource.WithPretty())
	if err != nil {
		return nil, fmt.Errorf("cannot read entry: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("bad connection: %v", err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body: %v", err)
	}
	return resp, nil
}

func read(es *elasticsearch.Client, index, id string) (*ResponseBody, error) {
	res, err := es.Get(index, id,
		//Es.Get.WithSourceIncludes("text,user"),
		es.Get.WithPretty())
	if err != nil {
		return nil, fmt.Errorf("cannot read entry: %v", err)
	}
	if res.IsError() {
		return nil, fmt.Errorf("bad connection: %v", err)
	}

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body: %v", err)
	}

	var rb *ResponseBody
	if err := json.Unmarshal(resp, &rb); err != nil {
		return rb, fmt.Errorf("response contains bad json: %v", err)
	}
	return rb, nil
}
