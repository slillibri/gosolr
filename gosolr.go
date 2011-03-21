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
    Body    string
    Authkey string
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
    }
    if req.Method == "POST" {
        header := req.Header

        //Reject non-json data
        ct := header["Content-Type"][0]
        if ct != "application/json" {
            l4g.Error("Unsupported Content type %s", ct)
            http.Error(w, "Unsupported content type", 400)
            return
        }

        //Handle length check
        length, _ := strconv.Atoi(header["Content-Length"][0])
        if length > 1024*1024 {
            l4g.Error("Post too large: %d", length)
            http.Error(w, "Post too large", 400)
            return
        }
        l4g.Debug("Post content-length: %d", length)

        body := make([]byte, length)
        len, ok := io.ReadFull(req.Body, body)
        l4g.Debug("Content-type of %d, but read %d bytes from body", length, len)
        if ok != nil {
            l4g.Debug("error reading io.ReadFull: %s", ok.String())
            http.Error(w, "Internal Server Error", 500)
            return
        }
        l4g.Debug("io.ReadFull read %d bytes", len)

        var message SolrPost
        if ok := json.Unmarshal(body, &message); ok != nil {
            l4g.Debug("Error unmarshalling json: %s", ok.String())
            http.Error(w, "Internal Server Error", 500)
            return
        }

        l4g.Debug("JSON message body: %s", message.Body)

        //Handle Auth
        if solrServers[apiKey]["authstring"] != message.Authkey {
            l4g.Error("Incorrect authkey")
            http.Error(w, "Unauthorized", 401)
            return
        }

        //Post message to queue
        nc, err := net.Dial("tcp", "", config["stomp"]["host"])
        if err != nil {
            l4g.Error("Error conneceting to queue %s", err.String())
            http.Error(w, "Internal Server Error", 500)
            return
        }

        //Note, the stomp module doesn't return os.Error on Send()
        c := stomp.Connect(nc, nil)
        queue := fmt.Sprintf("/queue/%s", apiKey)
        l4g.Debug("Posting %s to queue %s", message.Body, queue)
        c.Send(queue, message.Body)
        c.Disconnect()
        
        //Return result to client
        http.Error(w, "ok", 200)
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
    
    var err os.Error
    var srv http.Server
    srv.Addr = fmt.Sprintf("%s:%s", config["default"]["host"], config["default"]["port"])
    srv.Handler = nil
    if srv.ReadTimeout, err = strconv.Atoi64(config["default"]["read_timeout"]); err != nil {
        l4g.Error("Configuration error. Bad read_timout value")
        os.Exit(1)
    }
    if srv.WriteTimeout, err = strconv.Atoi64(config["default"]["write_timeout"]); err != nil {
        l4g.Error("Configuration error. Bad write_timeout value")
        os.Exit(1)
    }
    
    //If this were real, this should be TLS
    if err := srv.ListenAndServe(); err != nil {
        l4g.Error("Error starting server: %s", err.String())
        os.Exit(1)
    }
}

func prettyPrint(printMap map[string]map[string]string) {
    for key, tmpMap := range printMap {
        for key2, value := range tmpMap {
            l4g.Debug("%s ->\t%s -> %s", key, key2, value)
        }
    }
}