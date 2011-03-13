//This is a monolithic proxy I am writing for a solr-as-service thing I am working on
//Probably unnecessary but it's fun learning new languages.
//Note, probably nothing in this is the Go way

package main

import (
    "flag"
    "http"
    "fmt"
    "io"
    "json"
    "os"
    "strconv"
    "stomp"
    "net"
    l4g "log4go.googlecode.com/hg"
)

var(
    config map[string]map[string]string
    solrServers map[string]map[string]string
)

type SolrPost struct{
    Apikey  string
    Body    string
    
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
    apiKey := req.URL.Path[1:]
    if req.Method == "GET" {
        //Need to have some cleanup and validation here
        solrUrl := fmt.Sprintf("http://%s/solr/%s/select/?%s", solrServers[apiKey]["server"], solrServers[apiKey]["core"], req.URL.RawQuery)
        l4g.Debug("Proxying Request to %s for %s", solrUrl, apiKey)
        
        r, _, err := http.Get(solrUrl)
        defer r.Body.Close()
        
        l4g.Debug("Tomcat response: %s", r.Status)
        
        if err != nil {
            //Set actual error page here
            l4g.Error("Error: %s\n", err.String())
            http.Error(w, "500 Internal Server Error", 500)
            return
        }
        r.Write(w)
        return
    }
    if req.Method == "POST" {
        header := req.Header
        
        //Reject non-json data
        ct := header["Content-Type"][0]
        if ct != "application/json" {
            l4g.Error("Unsupported Content type %s", ct)
            http.Error(w, "400 Unsupported format", 400)
            return
        }
        
        //Handle length check
        length, _ := strconv.Atoi(header["Content-Length"][0])
        if length > 1024*1024 {
            l4g.Error("Post too large: %d", length)
            http.Error(w, "400 Post too large", 400)
            return
        }
        l4g.Debug("Post content-length: %d", length)
        
        //TODO handle this error condition.
        body := make([]byte, length)
        len, _ := io.ReadFull(req.Body, body)
        l4g.Debug("io.ReadFull read %d bytes", len)

        var message SolrPost
        json.Unmarshal(body, &message)
        l4g.Debug("JSON message body: %s", message.Body)
        
        //Post message to queue
        nc, err := net.Dial("tcp", "", config["stomp"]["host"])
        if err != nil {
            l4g.Error("Error conneceting to queue %s", err.String())
            return
        }
        c := stomp.Connect(nc, nil)
        queue := fmt.Sprintf("/queue/%s", message.Apikey)
        l4g.Debug("Posting %s to queue %s", message.Body, queue)
        c.Send(queue, message.Body)
        c.Disconnect()
        
        //Return result to client
        resp := fmt.Sprintf("Post content %s", body)
        http.Error(w, resp, 501)
    }
    
}

func main() {
    //Load logging configuration
    l4g.LoadConfiguration("logging.xml")
    
    //Load default config file
    var configPath string
    flag.StringVar(&configPath, "config", "/etc/gosolr/gosolr.cfg", "Path to the configuration file")
    flag.Parse()
    config = loadConfig(configPath)
    
    //Load solr servers/cores from mysql db
    solrServers = loadSolrServers(config)
    prettyPrint(solrServers)
    
    //Setup the http proxy stuff
    for apiKey := range solrServers {
        urlPath := fmt.Sprintf("/%s", apiKey)
        http.HandleFunc(urlPath, handleRequest)
    }
    
    var srv http.Server
    srv.Addr = fmt.Sprintf("%s:%s", config["default"]["host"], config["default"]["port"])
    srv.Handler = nil
    srv.ReadTimeout, _ = strconv.Atoi64(config["default"]["read_timeout"])
    srv.WriteTimeout, _ = strconv.Atoi64(config["default"]["write_timeout"])
    
    l4g.Debug("%s", srv.Addr)
    if err := srv.ListenAndServe(); err != nil {
        l4g.Error("Error starting server: %s", err.String())
        os.Exit(1)
    }
}

func prettyPrint(printMap map[string]map[string]string) {
    for key, tmpMap := range printMap {
        fmt.Printf("%s -> ", key)
        for key2, value := range tmpMap {
            fmt.Printf("\t%s -> %s\n", key2, value)
        }
    }
}