package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
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

// $ curl -i -X POST -T /path/to/data.csv http://localhost:8080/test/upload-suggestion
// $ curl -i -d '{"name": "foo bar"}' http://localhost:8080/test/select-suggestion

var indexDB = &index{
	store: make(map[string]bleve.Index, 4),
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "uri")
	flag.Parse()

	err := startServer(*addr, setupHandler(http.DefaultServeMux))
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
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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
	idxACTru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
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
	idxACTua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
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
		case "act":
			idxACTru.Index(rec[i][1], strings.TrimSpace(rec[i][2]))
			idxACTua.Index(rec[i][1], strings.TrimSpace(rec[i][3]))
		case "org":
			idxORGru.Index(rec[i][1], strings.TrimSpace(rec[i][2]))
			idxORGua.Index(rec[i][1], strings.TrimSpace(rec[i][3]))
		}

	}

	indexDB.setIndex("atc-ru", idxATCru)
	indexDB.setIndex("inf-ru", idxINFru)
	indexDB.setIndex("inn-ru", idxINNru)
	indexDB.setIndex("act-ru", idxACTru)
	indexDB.setIndex("org-ru", idxORGru)

	indexDB.setIndex("atc-ua", idxATCua)
	indexDB.setIndex("inf-ua", idxINFua)
	indexDB.setIndex("inn-ua", idxINNua)
	indexDB.setIndex("act-ua", idxACTua)
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
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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
	if v.Limit == 0 {
		v.Limit = 10
	}

	idxATC := "atc-ru"
	idxINF := "inf-ru"
	idxINN := "inn-ru"
	idxACT := "act-ru"
	idxORG := "org-ru"
	if langUA(r.Header) {
		idxATC = "atc-ua"
		idxINF = "inf-ua"
		idxINN = "inn-ua"
		idxACT = "act-ua"
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
	mACT, err := findByName(idxACT, v.Name, v.Limit)
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
		convName = convString(v.Name, "en", "uk")
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
	if len(mACT) == 0 {
		mACT, err = findByName(idxACT, convName, v.Limit)
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
	sACT := make([]string, 0, len(mACT))
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
	for k := range mACT {
		sACT = append(sACT, k)
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
	c.SortStrings(sACT)
	c.SortStrings(sORG)

	res := result{Find: v.Name, Limit: v.Limit}
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
		if strings.HasPrefix(strings.ToLower(s.Name), strings.ToLower(convName)) {
			res.SuggINF1 = append(res.SuggINF1, s)
		} else {
			res.SuggINF2 = append(res.SuggINF2, s)
		}
	}
	for i := range sINN {
		s := sugg{Name: sINN[i]}
		s.Keys = append(s.Keys, mINN[s.Name]...)
		res.SuggINN = append(res.SuggINN, s)
	}
	for i := range sACT {
		s := sugg{Name: sACT[i]}
		s.Keys = append(s.Keys, mACT[s.Name]...)
		res.SuggACT = append(res.SuggACT, s)
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
	Find     string `json:"find,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	SuggATC  []sugg `json:"sugg_atc,omitempty"`
	SuggINF1 []sugg `json:"sugg_inf1,omitempty"`
	SuggINF2 []sugg `json:"sugg_inf2,omitempty"`
	SuggINN  []sugg `json:"sugg_inn,omitempty"`
	SuggACT  []sugg `json:"sugg_act,omitempty"`
	SuggORG  []sugg `json:"sugg_org,omitempty"`
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

var mapKB = map[string][]rune{
	"en": []rune("qwertyuiop[]\\asdfghjkl;'zxcvbnm,./`QWERTYUIOP{}|ASDFGHJKL:\"ZXCVBNM<>?~!@#$%^&*()_+"),
	"ru": []rune("йцукенгшщзхъ\\фывапролджэячсмитьбю.ёЙЦУКЕНГШЩЗХЪ/ФЫВАПРОЛДЖЭЯЧСМИТЬБЮ,Ё!\"№;%:?*()_+"),
	"uk": []rune("йцукенгшщзхї\\фівапролджєячсмитьбю.'ЙЦУКЕНГШЩЗХЇ/ФІВАПРОЛДЖЄЯЧСМИТЬБЮ,₴!\"№;%:?*()_+"),
}

func convString(s, from, to string) string {
	lang1 := mapKB[from]
	lang2 := mapKB[to]
	if lang1 == nil || lang2 == nil {
		return s
	}

	src := []rune(s)
	res := make([]rune, len(src))
	for i := range src {
		for j := range lang1 {
			if lang1[j] == src[i] {
				res[i] = lang2[j]
				break
			}
			res[i] = src[i]
		}
	}
	return string(res)
}
