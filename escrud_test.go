package escrud

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestCreateSource(t *testing.T) {
	err := Create("2", []byte(`{
			"user": "barsuk",
			"aim": "test Es",
			"text": "как изменилась сеть?"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Source("2")
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
	err := Create(id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "наверное, как-то изменилась."
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Delete("3")
	if err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}

	if got.Result != "deleted" {
		t.Errorf("doc %s has not been deleted: %v", id, err)
	}
}

func TestCreateRemoveArrayItemDelete(t *testing.T) {
	id := "test-3asdfasdfasdf3"
	err := Create(id, []byte(`{
			"mask_articles":[{"article_id":1886671,"position":3},{"article_id":1886746,"position":1}]
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}
	//t.Errorf("created\n")
	//return

	rmMe := 1886746
	upd, err := RemoveArrayItem("gl", id, "mask_articles", "article_id", rmMe)
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
		return
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
		return
	}

	got, err := Source(id)
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

	if _, err = Delete(id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreatePartialUpdate(t *testing.T) {
	id := "test-2asdfasdfasdf2"
	err := Create(id, []byte(`{
			"user": "slivki",
			"aim": "test partial update",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	s := `{"text": "ОТ ТАКОЙ ТЕКСТИЩЕ ТЕПЕРЬ ВЗАМЕН!"}`
	upd, err := Update(id, []byte(s))
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
		return
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
		return
	}

	got, err := Source(id)
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

	if _, err = Delete(id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateExists(t *testing.T) {
	id := "asdfasfsafsdfsd-asdf_asdfasf4"

	if ok, err := Exists("gl", id); ok {
		if err != nil {
			fmt.Printf("тут ещё какая-то ошибка: %v", err)
		}
		t.Errorf("there should not exist index with such an id")
	}

	err := Create(id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	if ok, err := Exists("gl", id); !ok {
		if err != nil {
			fmt.Printf("тут ещё какая-то ошибка: %v", err)
		}
		t.Errorf("now there must be exist such an id")
	}

	if _, err = Delete(id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateUpdate(t *testing.T) {
	id := "asdfasfsafsdfsd-asdf_asdfasf4"
	err := Create(id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	upd, err := Update(id, []byte(`{
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

	got, err := Source(id)
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

	if _, err = Delete(id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateRead(t *testing.T) {
	id := "asdfasdfasdfsdf-asfasdf_asdfasdfas5"
	err := Create(id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "наверное, как-то изменилась."
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Read(id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	source := (got.Source).(map[string]interface{})
	if source["user"] != "slivki" {
		t.Errorf("should be `slivki`! But : %s", source["user"])
	}

	if _, err = Delete(id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func _Read(t *testing.T) {
	got, err := Read(`A_rZ528BFcFwXbplhTED`)
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	fmt.Println(got.Source)
}
