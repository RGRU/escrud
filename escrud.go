// Package escrud provides simple CRUD operations and search function to work with Elastic Search
package escrud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
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
func Connect(host string, port int, scheme string) (*Client, error) {
	var err error
	//Es, err = elasticsearch.NewDefaultClient()
	esServer := fmt.Sprintf("%s://%s:%d", scheme, host, port)
	cfg := elasticsearch.Config{
		Addresses: []string{esServer},
		Transport: &Transport{}, // https://github.com/elastic/go-elasticsearch/blob/master/_examples/fasthttp/fasthttp.go
	}
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

// BulkCreate let's bulky index multiple entries by single request to Elastic.
// look full documentation here: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-example
func (Es *Client) BulkCreate(datum []byte) error {
	if len(datum) < 2 {
		return fmt.Errorf("empty data")
	}

	es := Es.Client

	res, err := es.Bulk(
		bytes.NewReader(datum),
		//es.Bulk.WithPretty(),
	)
	if err != nil {
		return fmt.Errorf("cannot bulky create entries: %v", err)
	}
	defer func() {
		err := res.Body.Close()
		if err != nil {
			log.Printf("cannot close response body: %v", err)
		}
	}()

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("cannot read response body: %v", err)
	}

	if res.IsError() {
		return fmt.Errorf("create item failed: %s", resp)
	}

	var rb ResponseBody
	if err := json.Unmarshal(resp, &rb); err != nil {
		return fmt.Errorf("response contains bad json: %v", err)
	}

	return nil
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

// IncrementField пересчитать просмотры в материале
// аналог запроса
// POST http://localhost:9200/article/_update/{{id}}/
// { "script" : "ctx._source.viewed+={{amount}}" }
func (Es *Client) IncrementField(index string, docID string, fieldName string, incr int) (*ResponseBody, error) {
	templ := fmt.Sprintf(`
{
  "script": {
    "source": "ctx._source.%s+=params.id",
	"params": {
		"id": %d
	}
  }
}
`, fieldName, incr)

	var upd *ResponseBody
	res, err := Es.Client.Update(
		index,
		docID,
		bytes.NewReader([]byte(templ)),
	)
	if err != nil {
		return upd, fmt.Errorf("cannot update entry: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return upd, fmt.Errorf("increment failed. Status: %s, err: %v", res.Status(), err)
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

// InsertArrayItem добавить элемент массива. Массива может не быть - тогда добавить и массив
// POST http://localhost:9200/mask/_update/_3/
// "script": "if (!ctx._source.containsKey(\"attending\")) { ctx._source.attending = newField }",
func (Es *Client) InsertArrayItem(index string, docID string, arrayName string, elem []byte) (*ResponseBody, error) {
	templ := fmt.Sprintf(`
{
  "script": {
    "source": "if (!ctx._source.containsKey(\"%[1]s\")) { ctx._source.%[1]s = [params.new] } else { ctx._source.%[1]s.add(params.new) }",
    "params": {
		"new": %[2]s
    }
  }
}
`, arrayName, fmt.Sprintf("%s", elem))

	var upd *ResponseBody
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
		return upd, fmt.Errorf("insert failed. Status: %s, err: %v", res.Status(), err)
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

// UpdateArrayItem заменить элемент массива по его параметру (пока только числовой ID)
// POST http://localhost:9200/mask/_update/_3/
// { "script": { "inline": "ctx._source.mask_articles.removeIf(li -> li.article_id == params.article_id);", "lang": "painless", "params": { "article_id": 1886746, "cat": { "article_id": 1886746, "position": 5 } } } }
func (Es *Client) UpdateArrayItem(index string, docID string, arrayName string, itemName string, itemValue int, subst []byte) (*ResponseBody, error) {
	templ := fmt.Sprintf(`
{
  "script": {
    "source": "for (def i = 0; i < ctx._source.%[1]s.length; i++) {if (ctx._source.%[1]s[i].%[2]s == params.id) {ctx._source.%[1]s[i] = params.replace;}} ",
    "params": {
		"id": %[3]d,
		"replace": %[4]s
    }
  }
}
`, arrayName, itemName, itemValue, fmt.Sprintf("%s", subst))

	var upd *ResponseBody
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
		return upd, fmt.Errorf("update item failed. Status: %s, err: %v", res.Status(), err)
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

// RemoveArrayItem удалить элемент массива по его параметру (пока только числовой ID)
// POST http://localhost:9200/mask/_update/_3/
// { "script": { "source": "ctx._source.mask_articles.removeIf(li -> li.article_id == params.article_id)", "params": { "article_id": 1886746 } } }
func (Es *Client) RemoveArrayItem(index string, docID string, arrayName string, itemName string, itemValue int) (*ResponseBody, error) {
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

	var upd *ResponseBody
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
		return upd, fmt.Errorf("remove item failed. Status: %s, err: %v", res.Status(), err)
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
		return nil, fmt.Errorf("update failed. Status: %s, err: %v", res.Status(), err)
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
		return fmt.Errorf("create item failed: %s", resp)
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
		return nil, fmt.Errorf("remove item failed: %v", err)
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
		return nil, fmt.Errorf("get source failed: %v", err)
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
		return nil, fmt.Errorf("read failed: %v", err)
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
