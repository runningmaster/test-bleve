package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

const defaultAddr = "http://localhost:8080"

var indexDB = &index{
	store: make(map[string]bleve.Index, 4),
}

func main() {
	err := startServer(defaultAddr, setupHandler(http.DefaultServeMux))
	if err != nil {
		log.Fatalln(err)
	}
}

func setupHandler(m *http.ServeMux) http.Handler {
	m.HandleFunc("/test/upload-suggestion", uploadSuggestion)
	m.HandleFunc("/test/select-suggestion", selectSuggestion)
	return m
}

func startServer(a string, h http.Handler) error {
	u, err := url.Parse(a)
	if err != nil {
		return err
	}

	s := &http.Server{
		Addr:    u.Host,
		Handler: h,
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	go listenForShutdown(s, ch)

	return s.ListenAndServe()
}

func listenForShutdown(s *http.Server, ch <-chan os.Signal) {
	log.Printf("now ready to accept connections on %s", s.Addr)
	<-ch
	log.Printf("trying to shutdown...")

	ctx := context.Background()
	err := s.Shutdown(ctx)
	if err != nil {
		log.Printf("%v", err)
	}
}

func uploadSuggestion(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = r.Body.Close()

	rec, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idxATCru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idxINFru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idxINNru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idxORGru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idxATCua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idxINFua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idxINNua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	idxORGua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for i := range rec {
		if len(rec[i]) < 4 {
			err = fmt.Errorf("invalid csv: got %d, want %d", len(rec[i]), 4)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		switch rec[i][0] {
		case "atc":
			// fucking workaround
			key := rec[i][1] + "|" + strings.TrimSpace(strings.Replace(strings.Split(rec[i][2], "|")[0], " ", "", -1))
			idxATCru.Index(key, strings.TrimSpace(strings.Replace(rec[i][2], "|", " ", 1)))
			idxATCua.Index(key, strings.TrimSpace(strings.Replace(rec[i][3], "|", " ", 1)))
		case "info":
			idxINFru.Index(rec[i][1], strings.TrimSpace(rec[i][2]))
			idxINFua.Index(rec[i][1], strings.TrimSpace(rec[i][3]))
		case "inn":
			idxINNru.Index(rec[i][1], strings.TrimSpace(rec[i][2]))
			idxINNua.Index(rec[i][1], strings.TrimSpace(rec[i][3]))
		case "org":
			idxORGru.Index(rec[i][1], strings.TrimSpace(rec[i][2]))
			idxORGua.Index(rec[i][1], strings.TrimSpace(rec[i][3]))
		}

	}

	indexDB.setIndex("atc-ru", idxATCru)
	indexDB.setIndex("inf-ru", idxINFru)
	indexDB.setIndex("inn-ru", idxINNru)
	indexDB.setIndex("org-ru", idxORGru)

	indexDB.setIndex("atc-ua", idxATCua)
	indexDB.setIndex("inf-ua", idxINFua)
	indexDB.setIndex("inn-ua", idxINNua)
	indexDB.setIndex("org-ua", idxORGua)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, len(rec))
}

func selectSuggestion(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = r.Body.Close()

	v := struct {
		Name  string `json:"name"`
		Limit int    `json:"limit"`
	}{}

	err = json.Unmarshal(b, &v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	n := len([]rune(v.Name))
	if n <= 2 {
		err = fmt.Errorf("too few characters: %d", n)
	} else if n > 128 {
		err = fmt.Errorf("too many characters: %d", n)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	idxATC := "atc-ru"
	idxINF := "inf-ru"
	idxINN := "inn-ru"
	idxORG := "org-ru"
	if langUA(r.Header) {
		idxATC = "atc-ua"
		idxINF = "inf-ua"
		idxINN = "inn-ua"
		idxORG = "org-ua"
	}

	mATC, err := findByName(idxATC, v.Name, v.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mINF, err := findByName(idxINF, v.Name, v.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mINN, err := findByName(idxINN, v.Name, v.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mORG, err := findByName(idxORG, v.Name, v.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	convName := convString(v.Name, "en", "ru")
	if langUA(r.Header) {
		convName = convString(v.Name, "en", "ua")
	}
	if len(mATC) == 0 {
		mATC, err = findByName(idxATC, convName, v.Limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if len(mINF) == 0 {
		mINF, err = findByName(idxINF, convName, v.Limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if len(mINN) == 0 {
		mINN, err = findByName(idxINN, convName, v.Limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if len(mORG) == 0 {
		mORG, err = findByName(idxORG, convName, v.Limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sATC := make([]string, 0, len(mATC))
	sINF := make([]string, 0, len(mINF))
	sINN := make([]string, 0, len(mINN))
	sORG := make([]string, 0, len(mORG))

	for k := range mATC {
		sATC = append(sATC, k)
	}
	for k := range mINF {
		sINF = append(sINF, k)
	}
	for k := range mINN {
		sINN = append(sINN, k)
	}
	for k := range mORG {
		sORG = append(sORG, k)
	}

	// Sorting
	c := collate.New(language.Russian)
	if langUA(r.Header) {
		c = collate.New(language.Ukrainian)
	}
	c.SortStrings(sATC)
	c.SortStrings(sINF)
	c.SortStrings(sINN)
	c.SortStrings(sORG)

	res := result{Find: v.Name}
	for i := range sATC {
		s := sugg{Name: sATC[i]}
		// fucking workaround
		keys := mATC[s.Name]
		for i := range keys {
			keys[i] = strings.Split(keys[i], "|")[0]
		}
		s.Keys = append(s.Keys, keys...)
		res.SuggATC = append(res.SuggATC, s)
	}
	for i := range sINF {
		s := sugg{Name: sINF[i]}
		s.Keys = append(s.Keys, mINF[s.Name]...)
		res.SuggINF = append(res.SuggINF, s)
	}
	for i := range sINN {
		s := sugg{Name: sINN[i]}
		s.Keys = append(s.Keys, mINN[s.Name]...)
		res.SuggINN = append(res.SuggINN, s)
	}
	for i := range sORG {
		s := sugg{Name: sORG[i]}
		s.Keys = append(s.Keys, mORG[s.Name]...)
		res.SuggORG = append(res.SuggORG, s)
	}

	b, err = json.MarshalIndent(res, "", "\t")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, string(b))
}

type result struct {
	Find    string `json:"find,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	SuggATC []sugg `json:"sugg_atc,omitempty"`
	SuggINF []sugg `json:"sugg_inf,omitempty"`
	SuggINN []sugg `json:"sugg_inn,omitempty"`
	SuggORG []sugg `json:"sugg_org,omitempty"`
}

type sugg struct {
	Name string   `json:"name,omitempty"`
	Keys []string `json:"keys,omitempty"`
}

func langUA(h http.Header) bool {
	l := h.Get("Accept-Language")
	return strings.Contains(l, "uk") || strings.Contains(l, "ua") // FIXME
}

func findByName(key, name string, limit int) (map[string][]string, error) {
	idx, err := indexDB.getIndex(key)
	if err != nil {
		return nil, err
	}

	str := strings.Split(strings.ToLower(name), " ")
	cns := make([]query.Query, len(str))
	for i, v := range str {
		q := bleve.NewWildcardQuery("*" + v + "*")
		cns[i] = q
	}

	qry := bleve.NewConjunctionQuery(cns...)
	req := bleve.NewSearchRequest(qry)
	if limit > 0 {
		req.Size = limit
	}
	res, err := idx.Search(req)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]string, len(res.Hits))
	for _, v := range res.Hits {
		doc, err := idx.Document(v.ID)
		if err != nil {
			return nil, err
		}
		out[string(doc.Fields[0].Value())] = append(out[string(doc.Fields[0].Value())], v.ID)
	}

	return out, nil
}

//type indexer interface {
//	getIndex(string) (bleve.Index, error)
//	setIndex(string, bleve.Index) error
//}

type index struct {
	sync.RWMutex
	store map[string]bleve.Index
}

func (i *index) getIndex(key string) (bleve.Index, error) {
	i.RLock()
	defer i.RUnlock()

	if i.store == nil {
		return nil, fmt.Errorf("index store is nil")
	}

	if idx, ok := i.store[key]; ok {
		return idx, nil
	}

	return nil, fmt.Errorf("index not found")
}

func (i *index) setIndex(key string, idx bleve.Index) error {
	i.Lock()
	defer i.Unlock()

	if i.store == nil {
		return fmt.Errorf("index store is nil")
	}

	if _, ok := i.store[key]; ok {
		delete(i.store, key)
	}

	i.store[key] = idx

	return nil
}

func convString(s, from, to string) string {
	return s
}

// $ curl -i -X POST -T ~/Downloads/SearchDataInit.csv http://localhost:8080/test/upload-suggestion
// $ curl -i -d '{"name": "рисперидон"}' http://localhost:8080/test/select-suggestion

/*
// copyright http://typing.su (sovtime.ru)

var langSymbols = {
    ru: 'йцукенгшщзхъ\\фывапролджэячсмитьбю.ёЙЦУКЕНГШЩЗХЪ/ФЫВАПРОЛДЖЭЯЧСМИТЬБЮ,Ё!"№;%:?*()_+',
    en: 'qwertyuiop[]\\asdfghjkl;\'zxcvbnm,./`QWERTYUIOP{}|ASDFGHJKL:"ZXCVBNM<>?~!@#$%^&*()_+',
    uk: 'йцукенгшщзхї\\фівапролджєячсмитьбю.\'ЙЦУКЕНГШЩЗХЇ/ФІВАПРОЛДЖЄЯЧСМИТЬБЮ,₴!"№;%:?*()_+',
    be: 'йцукенгшўзх\'\\фывапролджэячсмітьбю.ёЙЦУКЕНГШЎЗХ\'/ФЫВАПРОЛДЖЭЯЧСМІТЬБЮ,Ё!"№;%:?*()_+',
    uz: 'йцукенгшўзхъ\\фқвапролджэячсмитьбю.ёЙЦУКЕНГШЎЗХЪ/ФҚВАПРОЛДЖЭЯЧСМИТЬБЮ,Ё!"№;%:?*()ҒҲ',
    kk: 'йцукенгшщзхъ\\фывапролджэячсмитьбю№(ЙЦУКЕНГШЩЗХЪ/ФЫВАПРОЛДЖЭЯЧСМИТЬБЮ?)!ӘІҢҒ;:ҮҰҚӨҺ',
    // ka: 'ქწერტყუიოპ[]~ასდფგჰჯკლ;\'ზხცვბნმ,./„ ჭ ღთ     {}| შ    ჟ ₾:"ძ ჩ  N <>?“!@#$%^&*()_+',
    az: 'qüertyuiopöğ\\asdfghjklıəzxcvbnmçş.`QÜERTYUİOPÖĞ/ASDFGHJKLIƏZXCVBNMÇŞ,~!"№;%:?*()_+',
    lt: 'qwertyuiop[]\\asdfghjkl;\'zxcvbnm,./`QWERTYUIOP{}|ASDFGHJKL:"ZXCVBNM<>?~ĄČĘĖĮŠŲŪ()_Ž',
    mo: 'qwertyuiopăîâasdfghjklșțzxcvbnm,./„QWERTYUIOPĂÎÂASDFGHJKLȘȚZXCVBNM;:?”!@#$%^&*()_+',
    lv: 'qwertyuiop[]\\asdfghjkl;\'zxcvbnm,./`QWERTYUIOP{}|ASDFGHJKL:"ZXCVBNM<>?~!@#$%^&*()_+',//en
    ky: 'йцукенгшщзхъ\\фывапролджэячсмитьбю.ёЙЦУКЕНГШЩЗХЪ/ФЫВАПРОЛДЖЭЯЧСМИТЬБЮ,Ё!"№;%:?*()_+',
    tg: 'йқукенгшҳзхъ\\фҷвапролджэячсмитӣбю.ёЙҚУКЕНГШҲЗХЪ/ФҶВАПРОЛДЖЭЯЧСМИТӢБЮ,Ё!"№;%:?*()ҒӮ',
    hy: 'քոեռտըւիօպխծշասդֆգհյկլ;՛զղցվբնմ,․/՝ՔՈԵՌՏԸՒԻՕՊԽԾՇԱՍԴՖԳՀՅԿԼ։"ԶՂՑՎԲՆՄ<>՞՜ԷԹՓՁՋՒևՐՉՃ—Ժ',
    tk: 'äwertyuiopňöşasdfghjkl;\'züçýbnm,./žÄWERTYUIOPŇÖŞASDFGHJKL:"ZÜÇÝBNM<>?Ž!@#$%№&*()_+',
    et: 'qwertyuiopüõ\'asdfghjklöäzxcvbnm,.-ˇQWERTYUIOPÜÕ*ASDFGHJKLÖÄZXCVBNM;:_~!"#¤%&/()=?`'
};
var fromLang = '',
    toLang = '';

function convert(text, lang1, lang2) {
    var resultText = '';

    if (lang1 === 'auto') {
        if (langSymbols[lang2].indexOf(text.charAt(1)) === -1) {
            fromLang = langSymbols.en;
            toLang = langSymbols[lang2];
            // console.log('en', lang2);
        } else {
            fromLang = langSymbols[lang2];
            toLang = langSymbols.en;
            // console.log(lang2, 'en');
        }

    } else {
      fromLang = langSymbols[lang1];
      toLang = langSymbols[lang2];
    }

    for (var i = 0; i < text.length; i++) {
        var j = fromLang.indexOf(text.charAt(i));
        if (j < 0) {
            resultText += text.charAt(i);
        } else {
            resultText += toLang.charAt(j);
        }
    }
    return resultText;
}

function output(form) {
    form.decoded.value = convert(form.coded.value, form.lang1.value, form.lang2.value);
}
function clear(form) {
    form.decoded.value = '';
    form.coded.value = '';
}

function input(form, area, buttonStart, buttonClear, select1, select2) {
    area.addEventListener('input', function() {
        output(form);
    }, false);

    buttonStart.addEventListener('click', function() {
        output(form);
    }, false);

    buttonClear.addEventListener('click', function() {
        clear(form);
    }, false);

    select1.addEventListener('change', function() {
        output(form);
    }, false);

    select2.addEventListener('change', function() {
        output(form);
    }, false);
}

function onLoad() {
    var form = document.getElementById('convert-form'),
    area = form.coded,
    button1 = document.getElementById('start-btn'),
    button2 = document.getElementById('clear-btn'),
    select1 = document.getElementById('lang1'),
    select2 = document.getElementById('lang2');

    input(form, area, button1, button2, select1, select2);
}

window.onload = function() {
  onLoad();
};

*/
