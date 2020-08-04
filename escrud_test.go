package escrud

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

var Es = Conn()

// Connect to elasticsearch
func Conn() *Client {
	es, err := Connect(os.Getenv("ELASTIC"), 9200, "http")
	if err != nil {
		fmt.Println("Elasticsearch error:", err)
	}
	fmt.Println("Elasticsearch info:", es.Info)
	return es
}

func TestCreateSource(t *testing.T) {
	err := Es.Create("test", "2", []byte(`{
			"user": "barsuk",
			"aim": "test Es",
			"text": "как изменилась сеть?"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Es.Source("test", "2")
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	var parsed struct {
		Aim  string `json:"aim"`
		User string `json:"user"`
		Text string `json:"text"`
	}

	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Errorf("cannot parse json answer: %v", err)
	}

	if parsed.Text != "как изменилась сеть?" {
		t.Errorf("bad result: %v", err)
	}
}

func TestCreateDelete(t *testing.T) {
	id := "3puopiupoiupasdfdasfpoasfiu"
	err := Es.Create("test", id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "наверное, как-то изменилась."
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Es.Delete("test", id)
	if err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}

	if got.Result != "deleted" {
		t.Errorf("doc %s has not been deleted: %v", id, err)
	}
}

func TestCreateUpdateArrayItemAndDelete(t *testing.T) {
	id := "test-5asdfasdfasdf5"
	iname := "test"
	err := Es.Create(iname, id, []byte(`{
			"mask_articles":[{"article_id":1886671,"position":3},{"article_id":1886746,"position":1}]
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}
	//t.Errorf("created\n")
	//return

	updMe := 1886746
	upd, err := Es.UpdateArrayItem(iname, id, "mask_articles", "article_id", updMe, []byte(`
		{
        "article_id": 1886746,
        "position": 5
      }`))
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
		return
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
		return
	}

	got, err := Es.Source("test", id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	type article struct {
		ArticleID int `json:"article_id"`
		Position  int `json:"position"`
	}

	var parsed struct {
		Aim          string    `json:"aim"`
		User         string    `json:"user"`
		Text         string    `json:"text"`
		MaskArticles []article `json:"mask_articles"`
	}

	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Errorf("cannot parse json answer: %v", err)
	}

	//fmt.Printf("%+v\n", parsed)

	checker := false
	for _, a := range parsed.MaskArticles {
		if a.ArticleID == updMe && a.Position == 5 {
			checker = true
		}
	}
	if !checker {
		t.Errorf("update failed: %v\n", err)
	}

	//t.Fatal("good\n")

	if _, err = Es.Delete("test", id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateRemoveArrayItemDelete(t *testing.T) {
	id := "test-3asdfasdfasdf3"
	iname := "test"
	err := Es.Create(iname, id, []byte(`{
			"mask_articles":[{"article_id":1886671,"position":3},{"article_id":1886746,"position":1}]
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}
	//t.Errorf("created\n")
	//return

	rmMe := 1886746
	upd, err := Es.RemoveArrayItem(iname, id, "mask_articles", "article_id", rmMe)
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
		return
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
		return
	}

	got, err := Es.Source("test", id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	type article struct {
		ArticleID int `json:"article_id"`
		Position  int `json:"position"`
	}

	var parsed struct {
		Aim          string    `json:"aim"`
		User         string    `json:"user"`
		Text         string    `json:"text"`
		MaskArticles []article `json:"mask_articles"`
	}

	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Errorf("cannot parse json answer: %v", err)
	}

	//fmt.Printf("%+v\n", parsed)

	for _, a := range parsed.MaskArticles {
		if a.ArticleID == rmMe {
			t.Errorf("update failed: %v\n", err)
		}
	}

	if _, err = Es.Delete("test", id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreatePartialUpdate(t *testing.T) {
	id := "test-2asdfasdfasdf2"
	err := Es.Create("test", id, []byte(`{
			"user": "slivki",
			"aim": "test partial update",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	s := `{"text": "ОТ ТАКОЙ ТЕКСТИЩЕ ТЕПЕРЬ ВЗАМЕН!"}`
	upd, err := Es.Update("test", id, []byte(s))
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
		return
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
		return
	}

	got, err := Es.Source("test", id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	var parsed struct {
		Aim  string `json:"aim"`
		User string `json:"user"`
		Text string `json:"text"`
	}

	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Errorf("cannot parse json answer: %v", err)
	}

	if parsed.User != "slivki" {
		t.Errorf("update failed: %v", err)
	}

	if parsed.Aim != "test partial update" {
		t.Errorf("update failed: %v", err)
	}

	if parsed.Text != "ОТ ТАКОЙ ТЕКСТИЩЕ ТЕПЕРЬ ВЗАМЕН!" {
		t.Errorf("update failed: %v", err)
	}

	if _, err = Es.Delete("test", id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateExists(t *testing.T) {
	id := "asdfasfsafsdfsd-asdf_asdfasf4"

	if ok, err := Es.Exists("test", id); ok {
		if err != nil {
			fmt.Printf("тут ещё какая-то ошибка: %v", err)
		}
		t.Errorf("there should not exist index with such an id")
	}

	err := Es.Create("test", id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	if ok, err := Es.Exists("test", id); !ok {
		if err != nil {
			fmt.Printf("тут ещё какая-то ошибка: %v", err)
		}
		t.Errorf("now there must be exist such an id")
	}

	if _, err = Es.Delete("test", id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateUpdate(t *testing.T) {
	id := "asdfasfsafsdfsd-asdf_asdfasf4"
	err := Es.Create("test", id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	upd, err := Es.Update("test", id, []byte(`{
				"user": "slivki",
				"aim": "test read create",
				"text": "ОТ ТАКОЙ ТЕКСТИЩЕ ТЕПЕРЬ ВЗАМЕН!"
			}
		`))
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
	}

	got, err := Es.Source("test", id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	var parsed struct {
		Aim  string `json:"aim"`
		User string `json:"user"`
		Text string `json:"text"`
	}

	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Errorf("cannot parse json answer: %v", err)
	}

	if parsed.Text != "ОТ ТАКОЙ ТЕКСТИЩЕ ТЕПЕРЬ ВЗАМЕН!" {
		t.Errorf("update failed: %v", err)
	}

	if _, err = Es.Delete("test", id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateRead(t *testing.T) {
	id := "asdfasdfasdfsdf-asfasdf_asdfasdfas5"
	err := Es.Create("test", id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "наверное, как-то изменилась."
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Es.Read("test", id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	source := (got.Source).(map[string]interface{})
	if source["user"] != "slivki" {
		t.Errorf("should be `slivki`! But : %s", source["user"])
	}

	if _, err = Es.Delete("test", id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func _Read(t *testing.T) {
	got, err := Es.Read("test", `A_rZ528BFcFwXbplhTED`)
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	fmt.Println(got.Source)
}
