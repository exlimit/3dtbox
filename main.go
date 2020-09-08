package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/cheggaaa/pb.v1"

	tile3d "github.com/flywave/go-3dtile"
	_ "github.com/shaxbee/go-spatialite"
)

var help bool
var basedir string
var chanNums int

var cnter int64
var level int
var db *sql.DB
var setlist []string
var wg sync.WaitGroup
var workers chan string

var bar *pb.ProgressBar

//fetchTileset get tileset.json
func fetchTileset(uri string, depth bool) {
	defer wg.Done()
	defer func() {
		<-workers
	}()

	time.Sleep(time.Microsecond * 200)

	t := time.Now()
	resp, err := http.Get(uri)
	if err != nil {
		log.Errorf("fetch :%s error, details: %s ~", uri, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Errorf("fetch %v tileset error, status code: %d ~", uri, resp.StatusCode)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("read tileset error ~ %s", err)
		return
	}
	if len(body) == 0 {
		log.Warnf("nil tileset %v ~", uri)
		return
	}
	u, err := url.Parse(uri)
	if err != nil {
		log.Errorf("parse :%s error, details: %s ~", uri, err)
		return
	}
	rawp := u.EscapedPath()
	dir := filepath.Join(basedir, filepath.Dir(rawp))
	os.MkdirAll(dir, os.ModePerm)
	fileName := filepath.Join(basedir, rawp)
	err = ioutil.WriteFile(fileName, body, os.ModePerm)
	if err != nil {
		log.Warnf("write tileset %v ~", err)
	}
	if depth {
		_, err = db.Exec("INSERT INTO tiles(u,t,s,c) values(?,?,?,?)", uri, 1, 0, 1)
		if err != nil {
			log.Errorf("insert tileset %s error, details:%v", uri, err)
		}
	}
	bdReader := bytes.NewReader(body)
	ts := tile3d.TilesetFromJson(bdReader)
	atomic.StoreInt64(&cnter, 0)
	for _, child := range ts.Root.Children {
		procChild(uri, child, depth)
	}

	_, err = db.Exec("UPDATE tiles SET s = 1, c = ? WHERE u = ?", cnter, uri)
	if err != nil {
		log.Errorf("update tileset %s status error, detail: %v", uri, err)
	}

	secs := time.Since(t).Seconds()
	fmt.Printf("tile %v, %.3fs, %.2f kb, %s ...\n", t, secs, float32(len(body))/1024.0, uri)
}

//FetchTile get tile
func fetchTile(uri string) {

	time.Sleep(time.Microsecond * 200)

	t := time.Now()
	resp, err := http.Get(uri)
	if err != nil {
		log.Errorf("fetch :%s error, details: %s ~", uri, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Errorf("fetch %v tile error, status code: %d ~", uri, resp.StatusCode)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("read tile error ~ %s", err)
		return
	}
	if len(body) == 0 {
		log.Warnf("nil tile %v ~", uri)
		return
	}

	u, err := url.Parse(uri)
	if err != nil {
		log.Errorf("parse :%s error, details: %s ~", uri, err)
		return
	}
	rawp := u.EscapedPath()
	dir := filepath.Join(basedir, filepath.Dir(rawp))
	os.MkdirAll(dir, os.ModePerm)
	fileName := filepath.Join(basedir, rawp)
	err = ioutil.WriteFile(fileName, body, os.ModePerm)
	if err != nil {
		log.Warnf("write tiles %v ~", err)
	}

	_, err = db.Exec("INSERT INTO tiles(u,t,s,c) values(?,?,?,?)", uri, 2, 1, 1)
	if err != nil {
		log.Errorf("insert %s error, details:%v", uri, err)
	}

	secs := time.Since(t).Seconds()
	fmt.Printf("tile %v, %.3fs, %.2f kb, %s ...\n", t, secs, float32(len(body))/1024.0, uri)
}

func procChild(baseuri string, tile tile3d.Tile, depth bool) {

	if tile.Content != nil {
		procContent(baseuri, tile.Content.Url, depth)
	}

	for _, child := range tile.Children {

		if child.Content != nil {
			procContent(baseuri, child.Content.Url, depth)
		}

		if child.Children != nil {
			for _, c := range child.Children {
				procChild(baseuri, c, depth)
			}
		}
	}
}

func procContent(buri, curi string, depth bool) {
	atomic.AddInt64(&cnter, 1)
	bu, err := url.Parse(buri)
	if err != nil {
		log.Error(err)
	}
	cu, err := url.Parse(curi)
	if err != nil {
		log.Error(err)
	}
	uri := bu.ResolveReference(cu).String()
	ext := filepath.Ext(cu.Path)
	switch strings.ToLower(ext) {
	case ".json":
		if depth {
			fetchTileset(uri, depth)
		} else {
			cnt := countTileset(uri)
			_, err = db.Exec("INSERT INTO tiles(u,t,s,c) values(?,?,?,?)", uri, 1, 0, cnt)
			if err != nil {
				log.Errorf("insert tileset  %s error, details: %v", uri, err)
			}
		}
	case ".b3dm", ".pnts", ".i3dm":
		fetchTile(uri)
	}
	bar.Increment()
}

//countTileset get tileset.json
func countTileset(uri string) int {
	var cnt int
	resp, err := http.Get(uri)
	if err != nil {
		log.Errorf("fetch :%s error, details: %s ~", uri, err)
		return cnt
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Errorf("fetch %v tileset error, status code: %d ~", uri, resp.StatusCode)
		return cnt
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("read tileset error ~ %s", err)
		return cnt
	}
	if len(body) == 0 {
		log.Warnf("nil tileset %v ~", uri)
		return cnt
	}
	bdReader := bytes.NewReader(body)
	ts := tile3d.TilesetFromJson(bdReader)
	for _, child := range ts.Root.Children {
		cnt += countChild(child)
	}
	return cnt
}

func countChild(tile tile3d.Tile) int {
	var cnt int
	if tile.Content != nil {
		cnt++
	}

	for _, child := range tile.Children {
		if child.Content != nil {
			cnt++
		}

		if child.Children != nil {
			for _, c := range child.Children {
				cnt += countChild(c)
			}
		}
	}
	return cnt
}

func startFetcher(depth bool) {
	rows, err := db.Query("SELECT u,c FROM tiles WHERE t=1 and s=0;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var url string
	var total int
	for rows.Next() {
		cnt := 0
		rows.Scan(&url, &cnt)
		total += cnt
		setlist = append(setlist, url)
	}

	prestr := ""
	if depth {
		log.Info("start dfs fetcher ~")
		prestr = "dfs fetcher"
	} else {
		log.Infof("start bfs fetcher, level %d ~", level)
		prestr = fmt.Sprintf("bfs fetcher, level %d", level)
	}

	bar = pb.New(total).Prefix(prestr).Postfix("\n")
	bar.SetRefreshRate(time.Second)
	bar.Start()

	workers = make(chan string, chanNums)

	for _, url := range setlist {
		select {
		case workers <- url:
			wg.Add(1)
			go fetchTileset(url, depth)
		}
	}
	wg.Wait()
	bar.FinishPrint("finished ~")
	if !depth {
		level++
		startFetcher(depth)
	}
}

func main() {

	flag.BoolVar(&help, "h", false, "Show this help")
	flag.StringVar(&basedir, "o", "./output", "Specify output directory")
	flag.IntVar(&chanNums, "c", 4, "Specify number of fetchers")
	flag.Usage = usage
	flag.Parse()
	if help {
		flag.Usage()
	}
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Please specify the root tileset url ~\n")
		flag.Usage()
		return
	}
	os.MkdirAll(basedir, os.ModePerm)
	var err error
	db, err = sql.Open("sqlite3", filepath.Join(basedir, "tiles.db"))
	if err != nil {
		log.Fatal(err)
	}
	// _, err = db.Exec("PRAGMA synchronous=0")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// _, err = db.Exec("PRAGMA locking_mode=EXCLUSIVE")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	_, err = db.Exec("PRAGMA journal_mode=DELETE")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("create table if not exists tiles (u text NOT NULL PRIMARY KEY, t integer, s integer,c integer);")
	if err != nil {
		log.Fatal(err)
	}

	for _, u := range args {
		var s, c int
		err = db.QueryRow("SELECT s,c FROM tiles WHERE t=1 and u= ?;", u).Scan(&s, &c)
		if err == sql.ErrNoRows {
			cnt := countTileset(u)
			_, err = db.Exec("INSERT INTO tiles(u,t,s,c) values(?,?,?,?)", u, 1, 0, cnt)
			if err != nil {
				log.Errorf("insert tileset %s error, details:%v", u, err)
				continue
			}
			fmt.Printf("url:%s, children:%d, status:%d\n", u, cnt, s)
		} else if err != nil {
			fmt.Printf("query url error, details: %v\n", err)
		} else {
			if s == 1 {
				fmt.Printf(" %s already fetched\n", u)
			} else {
				fmt.Printf("url:%s, children:%d, status:%d\n", u, c, s)
			}
		}
	}
	start := time.Now()
	startFetcher(false)
	log.Printf("finished, consuming : %fs\n", time.Since(start).Seconds())
}

func usage() {
	fmt.Fprintf(os.Stderr, `3dtbox version: 3dtbox/0.0.1
Usage: 3dtbox [-h] [-o directory] url

Options:
`)
	flag.PrintDefaults()
}
