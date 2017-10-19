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
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unicode"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// $ curl -i -X POST -T /path/to/data.csv http://localhost:8080/test/upload-sugg
// $ curl -i -d '{"name": "foo bar"}' http://localhost:8080/test/select-suggestion

var indexDB = &index{
	store: make(map[string]bleve.Index, 10),
	vault: make(map[string]*sync.Map, 10),
	sales: make(map[int]int, 10000),
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "uri")
	flag.Parse()

	err := startServer(*addr, setupHandler(http.DefaultServeMux))
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Bye!")
}

func setupHandler(m *http.ServeMux) http.Handler {
	m.HandleFunc("/test/upload-sugg", uploadSugg)
	m.HandleFunc("/test/upload-sugg2", uploadSugg2)
	m.HandleFunc("/test/select-sugg", selectSugg)
	m.HandleFunc("/test/select-suggestion", selectSuggestion)
	m.HandleFunc("/test/select-name", selectSuggestion)
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

	err = s.ListenAndServe()
	if err != nil && err == http.ErrServerClosed {
		return nil
	}
	return err
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

type baseDoc struct {
	ID   int    `json:"id,omitempty"`
	Kind string `json:"kind,omitempty"`
	Code string `json:"code,omitempty"`
	Name string `json:"name,omitempty"`
	Info int    `json:"info,omitempty"`
	Sale int    `json:"sale,omitempty"`
}

func internalServerError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
	log.Printf("err: %s", err.Error())
}

func uploadSugg(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		internalServerError(w, err)
		return
	}

	rec, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
	if err != nil {
		internalServerError(w, err)
		return
	}

	vltATCru := &sync.Map{}
	idxATCru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltINFru := &sync.Map{}
	idxINFru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltINNru := &sync.Map{}
	idxINNru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltACTru := &sync.Map{}
	idxACTru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltORGru := &sync.Map{}
	idxORGru, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}

	vltATCua := &sync.Map{}
	idxATCua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltINFua := &sync.Map{}
	idxINFua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltINNua := &sync.Map{}
	idxINNua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltACTua := &sync.Map{}
	idxACTua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}
	vltORGua := &sync.Map{}
	idxORGua, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		internalServerError(w, err)
		return
	}

	var lang string
	for i := range rec {
		if i == 0 {
			continue
		}
		if len(rec[i]) < 6 {
			err = fmt.Errorf("invalid csv: got %d, want %d", len(rec[i]), 6)
		}
		if err != nil {
			internalServerError(w, err)
			return
		}

		docRU := &baseDoc{}
		docRU.ID, _ = strconv.Atoi(rec[i][1])
		docRU.Kind = rec[i][0]
		docRU.Name = rec[i][2]
		docRU.Info, _ = strconv.Atoi(rec[i][4])
		//docRU.Sale, _ = strconv.Atoi(rec[i][5])

		docUA := &baseDoc{}
		docUA.ID, _ = strconv.Atoi(rec[i][1])
		docUA.Kind = rec[i][0]
		docUA.Name = rec[i][3]
		docUA.Info, _ = strconv.Atoi(rec[i][4])
		//docUA.Sale, _ = strconv.Atoi(rec[i][5])

		key := rec[i][1]
		// fucking workaround
		if docRU.Kind == "atc" {
			docRU.Code = strings.TrimSpace(strings.Split(docRU.Name, "|")[0])
			//docRU.Name = strings.TrimSpace(strings.Replace(docRU.Name, "|", " ", 1))
			key = key + "|" + strings.Replace(docRU.Code, " ", "", -1)
		}
		if docUA.Kind == "atc" {
			docUA.Code = strings.TrimSpace(strings.Split(docUA.Name, "|")[0])
			//docUA.Name = strings.TrimSpace(strings.Replace(docUA.Name, "|", " ", 1))
			key = key + "|" + strings.Replace(docUA.Code, " ", "", -1)
		}

		if docRU.Kind == "info" {
			docRU.Kind = "inf"
		}
		if docUA.Kind == "info" {
			docUA.Kind = "inf"
		}

		lang = rec[i][5]
		if lang == "RU" {
			switch docRU.Kind {
			case "atc":
				idxATCru.Index(key, docRU.Name)
				vltATCru.Store(key, docRU)
			case "inf":
				idxINFru.Index(key, docRU.Name)
				vltINFru.Store(key, docRU)
			case "inn":
				idxINNru.Index(key, docRU.Name)
				vltINNru.Store(key, docRU)
			case "act":
				idxACTru.Index(key, docRU.Name)
				vltACTru.Store(key, docRU)
			case "org":
				idxORGru.Index(key, docRU.Name)
				vltORGru.Store(key, docRU)
			}
		} else {
			switch docUA.Kind {
			case "atc":
				idxATCua.Index(key, docUA.Name)
				vltATCua.Store(key, docUA)
			case "inf":
				idxINFua.Index(key, docUA.Name)
				vltINFua.Store(key, docUA)
			case "inn":
				idxINNua.Index(key, docUA.Name)
				vltINNua.Store(key, docUA)
			case "act":
				idxACTua.Index(key, docUA.Name)
				vltACTua.Store(key, docUA)
			case "org":
				idxORGua.Index(key, docUA.Name)
				vltORGua.Store(key, docUA)
			}
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

	indexDB.setVault("atc-ru", vltATCru)
	indexDB.setVault("inf-ru", vltINFru)
	indexDB.setVault("inn-ru", vltINNru)
	indexDB.setVault("act-ru", vltACTru)
	indexDB.setVault("org-ru", vltORGru)

	indexDB.setVault("atc-ua", vltATCua)
	indexDB.setVault("inf-ua", vltINFua)
	indexDB.setVault("inn-ua", vltINNua)
	indexDB.setVault("act-ua", vltACTua)
	indexDB.setVault("org-ua", vltORGua)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, len(rec)-1)
}

func uploadSugg2(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		internalServerError(w, err)
		return
	}

	rec, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
	if err != nil {
		internalServerError(w, err)
		return
	}

	for i := range rec {
		if i == 0 {
			continue
		}
		if len(rec[i]) < 2 {
			err = fmt.Errorf("invalid csv: got %d, want %d", len(rec[i]), 2)
		}
		if err != nil {
			internalServerError(w, err)
			return
		}

		key, _ := strconv.Atoi(rec[i][0])
		val, _ := strconv.Atoi(rec[i][1])

		indexDB.sales[key] = val

	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, len(rec)-1, len(indexDB.sales))
}

func selectSuggestion(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		internalServerError(w, err)
		return
	}

	v := struct {
		Name string `json:"name"`
	}{}

	err = json.Unmarshal(b, &v)
	if err != nil {
		internalServerError(w, err)
		return
	}

	n := len([]rune(v.Name))
	if n <= 2 {
		err = fmt.Errorf("too few characters: %d", n)
	} else if n > 128 {
		err = fmt.Errorf("too many characters: %d", n)
	}
	if err != nil {
		internalServerError(w, err)
		return
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

	mATC, err := findByName(idxATC, v.Name, false)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mINF, err := findByName(idxINF, v.Name, false)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mINN, err := findByName(idxINN, v.Name, false)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mACT, err := findByName(idxACT, v.Name, false)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mORG, err := findByName(idxORG, v.Name, false)
	if err != nil {
		internalServerError(w, err)
		return
	}

	convName := convString(v.Name, "en", "ru")
	if langUA(r.Header) {
		convName = convString(v.Name, "en", "uk")
	}
	if len(mATC) == 0 {
		mATC, err = findByName(idxATC, convName, false)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mINF) == 0 {
		mINF, err = findByName(idxINF, convName, false)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mINN) == 0 {
		mINN, err = findByName(idxINN, convName, false)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mACT) == 0 {
		mACT, err = findByName(idxACT, convName, false)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mORG) == 0 {
		mORG, err = findByName(idxORG, convName, false)
		if err != nil {
			internalServerError(w, err)
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

	res := result{Find: v.Name}
	for i := range sATC {
		s := sugg{Name: sATC[i]}
		s.Keys = sortMagic(idxATC, mATC[s.Name]...)
		// fucking workaround
		//keys := mATC[s.Name]
		for i := range s.Keys {
			s.Keys[i] = strings.Split(s.Keys[i], "|")[0]
		}
		//s.Keys = append(s.Keys, keys...)
		//s.Keys = sortMagic(idxATC, s.Keys...)
		//
		s.Name = strings.TrimSpace(strings.Replace(s.Name, "|", " ", 1))
		res.SuggATC = append(res.SuggATC, s)
	}
	// fucking workaround
	s1 := sugg{}
	for i := range sINF {
		s1.Keys = append(s1.Keys, mINF[sINF[i]]...)
	}
	// fucking workaround
	s1.Keys = sortMagic(idxINF, s1.Keys...)
	res.SuggINF = append(res.SuggINF, s1)

	for i := range sINN {
		s := sugg{Name: sINN[i]}
		s.Keys = append(s.Keys, mINN[s.Name]...)
		s.Keys = sortMagic(idxINN, s.Keys...)
		res.SuggINN = append(res.SuggINN, s)
	}
	for i := range sACT {
		s := sugg{Name: sACT[i]}
		s.Keys = append(s.Keys, mACT[s.Name]...)
		s.Keys = sortMagic(idxACT, s.Keys...)
		res.SuggACT = append(res.SuggACT, s)
	}
	for i := range sORG {
		s := sugg{Name: sORG[i]}
		s.Keys = append(s.Keys, mORG[s.Name]...)
		s.Keys = sortMagic(idxORG, s.Keys...)
		res.SuggORG = append(res.SuggORG, s)
	}

	b, err = json.MarshalIndent(res, "", "\t")
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, string(b))
}

func sortMagic(key string, keys ...string) []string {
	if len(keys) < 2 {
		return keys
	}

	vlt, err := indexDB.getVault(key)
	if err != nil {
		return keys
	}

	tmp := make([]*baseDoc, 0, len(keys))
	for i := range keys {
		if v, ok := vlt.Load(keys[i]); ok {
			d := v.(*baseDoc)
			d.Sale = indexDB.sales[d.ID]
			//println(keys[i], d.Info, d.Sale)
			tmp = append(tmp, d)
		}
	}

	if len(tmp) == 0 {
		return keys
	}

	sort.Slice(tmp,
		func(i, j int) bool {
			if tmp[i].Info > tmp[j].Info {
				return true
			} else if tmp[i].Info < tmp[j].Info {
				return false
			}
			if tmp[i].Sale > tmp[j].Sale {
				return true
			} else if tmp[i].Sale < tmp[j].Sale {
				return false
			}
			return tmp[i].Name < tmp[j].Name
		},
	)

	out := make([]string, len(tmp))
	for i := range tmp {
		//	println(tmp[i].ID, tmp[i].Info, tmp[i].Sale)
		out[i] = strconv.Itoa(tmp[i].ID)
	}

	return out
}

func selectSugg(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		internalServerError(w, err)
		return
	}

	v := struct {
		Name string `json:"name"`
	}{}

	err = json.Unmarshal(b, &v)
	if err != nil {
		internalServerError(w, err)
		return
	}

	n := len([]rune(v.Name))
	if n <= 2 {
		err = fmt.Errorf("too few characters: %d", n)
	} else if n > 128 {
		err = fmt.Errorf("too many characters: %d", n)
	}
	if err != nil {
		internalServerError(w, err)
		return
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

	mATC, err := findByName(idxATC, v.Name, true)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mINF, err := findByName(idxINF, v.Name, true)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mINN, err := findByName(idxINN, v.Name, true)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mACT, err := findByName(idxACT, v.Name, true)
	if err != nil {
		internalServerError(w, err)
		return
	}
	mORG, err := findByName(idxORG, v.Name, true)
	if err != nil {
		internalServerError(w, err)
		return
	}

	convName := convString(v.Name, "en", "ru")
	if langUA(r.Header) {
		convName = convString(v.Name, "en", "uk")
	}
	if len(mATC) == 0 {
		mATC, err = findByName(idxATC, convName, true)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mINF) == 0 {
		mINF, err = findByName(idxINF, convName, true)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mINN) == 0 {
		mINN, err = findByName(idxINN, convName, true)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mACT) == 0 {
		mACT, err = findByName(idxACT, convName, true)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}
	if len(mORG) == 0 {
		mORG, err = findByName(idxORG, convName, true)
		if err != nil {
			internalServerError(w, err)
			return
		}
	}

	mAll := make(map[string]struct{}, len(mATC)+len(mINF)+len(mINN)+len(mACT)+len(mORG))
	for k := range mATC {
		mAll[strings.ToUpper(strings.TrimSpace(strings.Replace(strings.Split(k, "|")[1], "|", " ", 1)))] = struct{}{}
	}
	for k := range mINF {
		mAll[strings.ToUpper(k)] = struct{}{}
	}
	for k := range mINN {
		mAll[strings.ToUpper(k)] = struct{}{}
	}
	for k := range mACT {
		mAll[strings.ToUpper(k)] = struct{}{}
	}
	for k := range mORG {
		mAll[strings.ToUpper(k)] = struct{}{}
	}
	sAll := make([]string, 0, len(mAll))
	for k := range mAll {
		sAll = append(sAll, k)
	}

	// Sorting
	c := collate.New(language.Russian)
	if langUA(r.Header) {
		c = collate.New(language.Ukrainian)
	}
	c.SortStrings(sAll)

	res := result{Find: v.Name}
	for i := range sAll {
		if strings.HasPrefix(strings.ToLower(sAll[i]), strings.ToLower(convName)) {
			res.Sugg = append(res.Sugg, sAll[i])
		}
	}
	for i := range sAll {
		if !strings.HasPrefix(strings.ToLower(sAll[i]), strings.ToLower(convName)) {
			res.Sugg = append(res.Sugg, sAll[i])
		}
	}

	b, err = json.MarshalIndent(res, "", "\t")
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, string(b))
}

type result struct {
	Find    string   `json:"find,omitempty"`
	Sugg    []string `json:"sugg,omitempty"`
	SuggINF []sugg   `json:"sugg_inf,omitempty"`
	SuggINN []sugg   `json:"sugg_inn,omitempty"`
	SuggACT []sugg   `json:"sugg_act,omitempty"`
	SuggORG []sugg   `json:"sugg_org,omitempty"`
	SuggATC []sugg   `json:"sugg_atc,omitempty"`
}

type sugg struct {
	Name string   `json:"name,omitempty"`
	Keys []string `json:"keys,omitempty"`
}

func langUA(h http.Header) bool {
	l := h.Get("Accept-Language")
	return strings.Contains(l, "uk") || strings.Contains(l, "ua") // FIXME
}

func normName(s string) string {
	res := []rune(s)
	for i := range res {
		if !unicode.IsLetter(res[i]) {
			res[i] = ' '
		}
	}
	return string(res)
}

func findByName(key, name string, conj bool) (map[string][]string, error) {
	idx, err := indexDB.getIndex(key)
	if err != nil {
		return nil, err
	}

	name = normName(name)

	var qry query.Query
	if conj {
		str := strings.Split(strings.ToLower(name), " ")
		cns := make([]query.Query, len(str))
		for i, v := range str {
			q := bleve.NewWildcardQuery("*" + strings.TrimSpace(v) + "*")
			cns[i] = q
		}
		qry = bleve.NewConjunctionQuery(cns...)
	} else {
		qry = bleve.NewMatchPhraseQuery(strings.TrimSpace(name))
	}

	req := bleve.NewSearchRequest(qry)
	req.Size = 1000

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

type index struct {
	sync.RWMutex
	store map[string]bleve.Index
	vault map[string]*sync.Map
	sales map[int]int
}

func (i *index) getIndex(key string) (bleve.Index, error) {
	i.RLock()
	defer i.RUnlock()

	if i.store == nil {
		return nil, fmt.Errorf("index store is nil (%s)", key)
	}

	if idx, ok := i.store[key]; ok {
		return idx, nil
	}

	return nil, fmt.Errorf("index not found (%s)", key)
}

func (i *index) getVault(key string) (*sync.Map, error) {
	i.RLock()
	defer i.RUnlock()

	if i.vault == nil {
		return nil, fmt.Errorf("index vault is nil (%s)", key)
	}

	if vlt, ok := i.vault[key]; ok {
		return vlt, nil
	}

	return nil, fmt.Errorf("vault not found (%s)", key)
}

func (i *index) setIndex(key string, idx bleve.Index) error {
	i.Lock()
	defer i.Unlock()

	if i.store == nil {
		return fmt.Errorf("index store is nil (%s)", key)
	}

	if _, ok := i.store[key]; ok {
		delete(i.store, key)
	}

	i.store[key] = idx

	return nil
}

func (i *index) setVault(key string, vlt *sync.Map) error {
	i.Lock()
	defer i.Unlock()

	if i.vault == nil {
		return fmt.Errorf("index vault is nil (%s)", key)
	}

	if _, ok := i.vault[key]; ok {
		delete(i.vault, key)
	}

	i.vault[key] = vlt

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
