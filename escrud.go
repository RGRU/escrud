// Package escrud provides simple CRUD operations and search function to work with Elastic Search
package escrud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const IndexName = `gl`

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

func init() {
	var err error

	//Es, err = elasticsearch.NewDefaultClient()
	esServer := fmt.Sprintf("http://%s:9200", os.Getenv("ELASTIC"))
	cfg := elasticsearch.Config{Addresses: []string{esServer}}
	Es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s\n", err)
		os.Exit(1)
	}
	log.Println(elasticsearch.Version)
	info, err := Es.Info()
	if err != nil {
		log.Printf("Cannot get server info: %s\n", err)
		log.Printf("You should check Elastic health!")
		//os.Exit(1)
	}

	log.Println(info)
}

func Update(id string, data []byte) (Updated, error) {
	templ := []byte(`{"doc":`)
	templ = append(templ, data...)
	templ = append(templ, []byte(`}`)...)

	var upd Updated
	res, err := Es.Update(
		IndexName,
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

// Query lets construct and execute a search query
type Query struct {
	Size int // Size (Optional, integer) The number of hits to return. Defaults to 10.
	// id diap
	FilterRangeGreaterThan struct {
		On    bool
		Value int
	}
	FilterRangeLessThan struct {
		On    bool
		Value int
	}
	MustNotRangeIDGreaterThan struct {
		On    bool
		Value int
	}
	MustNotRangeIDLessThan struct {
		On    bool
		Value int
	}
	// icon
	HasIcon struct {
		On    bool
		Value bool
	}
	// video
	HasVideo struct {
		On    bool
		Value bool
	}
	// photoreport
	HasPhotorep struct {
		On    bool
		Value bool
	}
	// news or article
	IsNews struct {
		On    bool
		Value bool
	}
	// spiegel
	IsSpiegel struct {
		On    bool
		Value bool
	}
	// project
	Projects struct {
		On     bool
		Values []int
	}
	// rubric
	Rubrics struct {
		On     bool
		Values []int
	}
	// collection
	Collections struct {
		On     bool
		Values []int
	}
	// Tags
	Tags struct {
		On     bool
		Values []int
	}
	// Authors
	Authors struct {
		On     bool
		Values []int
	}
	// IsMainProject
	IsMainProject struct {
		On    bool
		Value bool
	}
	// IsMainColl
	IsMainColl struct {
		On    bool
		Value bool
	}
	// Type
	Type struct {
		On    bool
		Value string
	}
	// WordOr to find word OR word OR...
	WordOr struct {
		On    bool
		Value string
	}
	// Phrase to find exact phrase
	Phrase struct {
		On    bool
		Value string
	}
}

// String print
func (q *Query) String() string {
	query := fmt.Sprintf(queryTemplate,
		q.FilterRangeGreaterThan.Value,
		q.FilterRangeLessThan.Value,
		q.MustNotRangeIDGreaterThan.Value,
		q.MustNotRangeIDLessThan.Value,
		func() string {
			if q.HasIcon.On {
				return fmt.Sprintf(`,{"term":{"hasIcon":%t}}`, q.HasIcon.Value)
			}
			return ""
		}(),
		func() string {
			if q.HasPhotorep.On {
				return fmt.Sprintf(`,{"term":{"hasPhotorep":%t}}`, q.HasPhotorep.Value)
			}
			return ""
		}(),
		func() string {
			if q.HasVideo.On {
				return fmt.Sprintf(`,{"term":{"hasVideo":%t}}`, q.HasVideo.Value)
			}
			return ""
		}(),
		func() string {
			if q.IsNews.On {
				return fmt.Sprintf(`,{"term":{"isNews":%t}}`, q.IsNews.Value)
			}
			return ""
		}(),
		func() string {
			if q.IsSpiegel.On {
				return fmt.Sprintf(`,{"term":{"is_spiegel":%t}}`, q.IsSpiegel.Value)
			}
			return ""
		}(),
		func() string {
			if q.Projects.On {
				joined := strings.Trim(strings.Replace(fmt.Sprint(q.Projects.Values), " ", ",", -1), "[]")
				return fmt.Sprintf(`,{"terms":{"projects.id":[%s]}}`, joined)
			}
			return ""
		}(),
		func() string {
			if q.Rubrics.On {
				joined := strings.Trim(strings.Replace(fmt.Sprint(q.Rubrics.Values), " ", ",", -1), "[]")
				return fmt.Sprintf(`,{"terms":{"rubrics.id":[%s]}}`, joined)
			}
			return ""
		}(),
		func() string {
			if q.Collections.On {
				joined := strings.Trim(strings.Replace(fmt.Sprint(q.Collections.Values), " ", ",", -1), "[]")
				return fmt.Sprintf(`,{"terms":{"collections.id":[%s]}}`, joined)
			}
			return ""
		}(),
		func() string {
			if q.Authors.On {
				joined := strings.Trim(strings.Replace(fmt.Sprint(q.Authors.Values), " ", ",", -1), "[]")
				return fmt.Sprintf(`,{"terms":{"authors.id":[%s]}}`, joined)
			}
			return ""
		}(),
		func() string {
			if q.Tags.On {
				joined := strings.Trim(strings.Replace(fmt.Sprint(q.Tags.Values), " ", ",", -1), "[]")
				return fmt.Sprintf(`,{"terms":{"tags.id":[%s]}}`, joined)
			}
			return ""
		}(),
		func() string {
			if q.Collections.On && q.IsMainColl.On {
				return fmt.Sprintf(`,{"term":{"isMainColl":%t}}`, q.IsMainColl.Value)
			}
			return ""
		}(),
		func() string {
			if q.Projects.On && q.IsMainProject.On {
				return fmt.Sprintf(`,{"term":{"isMainProject":%t}}`, q.IsMainProject.Value)
			}
			return ""
		}(),
		func() string {
			if q.Type.On {
				return fmt.Sprintf(`,{"term":{"type":%q}}`, q.Type.Value)
			}
			return ""
		}(),
		func() string {
			if q.WordOr.On {
				return fmt.Sprintf(`,"must":{"match":{"text":%q}}`, q.WordOr.Value)
			}
			if q.Phrase.On {
				return fmt.Sprintf(`,"must":{"match_phrase":{"text":%q}}`, q.Phrase.Value)
			}
			return ""
		}(),
	)

	return query
}

const queryTemplate = `
		{
		  "query": {
			"bool": {
			  "filter": [
				{
				  "range": {
					"id": {
					  "gt": %[1]d,
					  "lt": %[2]d
					}
				  }
				}%[5]s%[6]s%[7]s%[8]s%[9]s%[10]s%[11]s%[12]s%[13]s%[14]s%[15]s%[16]s%[17]s
			  ],
			  "must_not": {
				"range": {
				  "id": [
					{
					  "gt": %[3]d,
					  "lt": %[4]d
					}
				  ]
				}
			  }
			  %[18]s
			}
		  }
		}
	`

// Search searches for a DSL query result
func (q *Query) Search() (result []byte, err error) {
	if q.Size < 1 {
		q.Size = 10
	}

	res, err := Es.Search(
		Es.Search.WithIndex(IndexName),
		Es.Search.WithBody(strings.NewReader(fmt.Sprint(q))),
		Es.Search.WithSize(q.Size),
		Es.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot search: %v", err)
	}
	defer res.Body.Close()

	resp, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body: %v", err)
	}

	if res.IsError() {
		return nil, fmt.Errorf("bad connection? %s", resp)
	}

	return resp, nil
}

// Create should contain a valid JSON with key {..."id":your_unique_id}
func Create(id string, data []byte) (err error) {
	if len(data) < 1 {
		data = []byte(fmt.Sprintf(`{"id":%s}`, id))
	}

	res, err := Es.Index(
		IndexName,
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

func Delete(id string) (Deleted, error) {
	var deleted Deleted
	res, err := Es.Delete(IndexName, id,
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

func Source(id string) ([]byte, error) {
	res, err := Es.GetSource(IndexName, id,
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

func Read(id string) (Got, error) {
	var got Got
	res, err := Es.Get(IndexName, id,
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
