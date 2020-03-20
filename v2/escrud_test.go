package escrud

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestCreateSource(t *testing.T) {
	err := Create("test","2", []byte(`{
			"user": "barsuk",
			"aim": "test Es",
			"text": "как изменилась сеть?"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Source("test","2")
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
	err := Create("test",id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "наверное, как-то изменилась."
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Delete("test",id)
	if err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}

	if got.Result != "deleted" {
		t.Errorf("doc %s has not been deleted: %v", id, err)
	}
}

func TestCreatePartialUpdate(t *testing.T) {
	id := "test-2asdfasdfasdf2"
	err := Create("test",id, []byte(`{
			"user": "slivki",
			"aim": "test partial update",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	s := `{"text": "ОТ ТАКОЙ ТЕКСТИЩЕ ТЕПЕРЬ ВЗАМЕН!"}`
	upd, err := Update("test",id, []byte(s))
	if err != nil {
		t.Errorf("cannot update id %s: %v", id, err)
		return
	}

	if upd.Result != "updated" {
		t.Errorf("cannot update id %s", id)
		return
	}

	got, err := Source("test",id)
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

	if _, err = Delete("test",id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateExists(t *testing.T) {
	id := "asdfasfsafsdfsd-asdf_asdfasf4"

	if ok, err := Exists("test", id); ok {
		if err != nil {
			fmt.Printf("тут ещё какая-то ошибка: %v", err)
		}
		t.Errorf("there should not exist index with such an id")
	}

	err := Create("test",id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	if ok, err := Exists("test", id); !ok {
		if err != nil {
			fmt.Printf("тут ещё какая-то ошибка: %v", err)
		}
		t.Errorf("now there must be exist such an id")
	}

	if _, err = Delete("test",id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateUpdate(t *testing.T) {
	id := "asdfasfsafsdfsd-asdf_asdfasf4"
	err := Create("test",id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "Вот такой текстовый текст"
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	upd, err := Update("test",id, []byte(`{
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

	got, err := Source("test",id)
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

	if _, err = Delete("test",id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func TestCreateRead(t *testing.T) {
	id := "asdfasdfasdfsdf-asfasdf_asdfasdfas5"
	err := Create("test",id, []byte(`{
			"user": "slivki",
			"aim": "test read create",
			"text": "наверное, как-то изменилась."
		}`))
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	got, err := Read("test",id)
	if err != nil {
		t.Errorf("cannot read id %s: %v", id, err)
	}

	source := (got.Source).(map[string]interface{})
	if source["user"] != "slivki" {
		t.Errorf("should be `slivki`! But : %s", source["user"])
	}

	if _, err = Delete("test",id); err != nil {
		t.Errorf("cannot delete id %s: %v", id, err)
	}
}

func _Read(t *testing.T) {
	got, err := Read("test",`A_rZ528BFcFwXbplhTED`)
	if err != nil {
		t.Errorf("ERR: %v", err)
	}

	fmt.Println(got.Source)
}