package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	tile3d "github.com/flywave/go-3dtile"
	_ "github.com/shaxbee/go-spatialite"
)

var counter int
var basedir string
var db *sql.DB
var wg sync.WaitGroup
var workers chan string

func procTileset(url string) error {
	file, err := os.OpenFile(url, os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	ts := tile3d.TilesetFromJson(file)
	for i := range ts.Root.Children {
		// procTile(url, &ts.Root.Children[i])
		fmt.Print(i)
	}
	ts.Asset.GltfUpAxis = "Y"
	transDefaut := true
	for _, v := range ts.Root.Transform {
		if v != 0 {
			transDefaut = false
		}
	}
	if transDefaut {
		ts.Root.Transform = [16]float64{
			1, 0, 0, 0,
			0, 0, -1, 0,
			0, 1, 0, 0,
			0, 0, 0, 1}
	}
	tsjson, err := ts.ToJson()
	if err != nil {
		return err
	}
	err = file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}
	n, err := file.WriteString(tsjson)
	if err != nil {
		return err
	}
	counter++
	log.Printf("rewrite %s ,  %d bytes~", url, n)
	return nil
}

//FetchTileset get tileset.json
func FetchTileset(uri string) {
	t := time.Now()
	_, err := db.Exec("INSERT INTO tiles(u,t,p) values(?,?,?)", uri, 1, 0)

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

	// _, err = db.Exec("INSERT INTO tiles(u,t,p) values(?,?,?)", uri, 1, 0)
	// 解析 计数 爬取subjson 写入列表
	bdReader := bytes.NewReader(body)
	ts := tile3d.TilesetFromJson(bdReader)
	for _, child := range ts.Root.Children {
		procChild(uri, child)
	}
	_, err = db.Exec("UPDATE tiles SET p = 1 WHERE u = ?", uri)

	secs := time.Since(t).Seconds()
	fmt.Printf("tile %v, %.3fs, %.2f kb, %s ...\n", t, secs, float32(len(body))/1024.0, uri)
	counter++
}

//FetchTile get tile
func FetchTile(uri string) {
	t := time.Now()
	_, err := db.Exec("INSERT INTO tiles(u,t,p) values(?,?,?)", uri, 2, 0)

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
	_, err = db.Exec("UPDATE tiles SET p = 1 WHERE u = ?", uri)

	secs := time.Since(t).Seconds()
	fmt.Printf("tile %v, %.3fs, %.2f kb, %s ...\n", t, secs, float32(len(body))/1024.0, uri)
	counter++
}

func procChild(baseuri string, tile tile3d.Tile) {
	if tile.Content != nil {
		procContent(baseuri, tile.Content.Url)
	}

	for _, child := range tile.Children {

		if child.Content != nil {
			procContent(baseuri, child.Content.Url)
		}

		if child.Children != nil {
			for _, c := range child.Children {
				procChild(baseuri, c)
			}
		}
	}
}

func procContent(buri, curi string) {
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
		FetchTileset(uri)
	case ".b3dm", ".pnts", ".i3dm":
		FetchTile(uri)
	}
	time.Sleep(time.Microsecond * 300)
}

func reFetchTile(uri string) {
	defer wg.Done()
	defer func() {
		<-workers
	}()

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

	_, err = db.Exec("UPDATE tiles SET p = 1 WHERE u = ?", uri)

	secs := time.Since(t).Seconds()
	fmt.Printf("tile %v, %.3fs, %.2f kb, %s ...\n", t, secs, float32(len(body))/1024.0, uri)
	counter++

}

func reFetcher() {
	rows, err := db.Query("SELECT u FROM tiles WHERE t=1 and p=0;")
	if err != nil {
		log.Fatal(err)
	}
	workers = make(chan string, 8)
	var url string
	for rows.Next() {
		rows.Scan(&url)
		time.Sleep(time.Second)
		select {
		case workers <- url:
			wg.Add(1)
			go reFetchTile(url)
		}
	}
	wg.Wait()
}
func upTileset() {
	dir := filepath.Join(basedir, "ge", "BulkMetadata")
	items, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Error(err)
	}
	baseurl := "http://lab.earthsdk.com/ge/BulkMetadata/"

	// var files []string
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		_, err = db.Exec("UPDATE tiles SET p = 1 WHERE u = ?", baseurl+item.Name())
		if err != nil {
			logrus.Error(err)
		}
	}

}

func main() {
	uri := "http://lab.earthsdk.com/ge/tileset.json"
	u, err := url.Parse(uri)
	basedir = "d:/tiler"
	os.MkdirAll(filepath.Join(basedir, filepath.Dir(u.Path)), os.ModePerm)
	db, err = sql.Open("sqlite3", filepath.Join(basedir, filepath.Dir(u.Path), "tiles.db"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("PRAGMA synchronous=0")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("PRAGMA locking_mode=EXCLUSIVE")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("PRAGMA journal_mode=DELETE")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("create table if not exists tiles (u text,t integer, p integer);")
	if err != nil {
		log.Fatal(err)
	}

	s := time.Now()
	// upTileset()
	reFetcher()
	// FetchTileset(uri)
	log.Printf("finished, proc %d files, times: %fs", counter, time.Since(s).Seconds())
}
