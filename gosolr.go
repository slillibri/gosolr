//This is a monolithic proxy I am writing for a solr-as-service thing I am working on
//Probably unnecessary but it's fun learning new languages.
//Note, probably nothing in this is the Go way

package main

import (
    "flag"
    "http"
    "fmt"
    "os"
    l4g "log4go.googlecode.com/svn/stable"
)

var(
    config map[string]string
    solrServers map[string]map[string]string
)

func handleRequest(w http.ResponseWriter, req *http.Request) {
    apiKey := req.URL.Path[1:]
    if req.Method == "GET" {        
        //Need to have some cleanup and validation here
        solrUrl := fmt.Sprintf("http://%s/solr/%s/select/?%s", solrServers[apiKey]["server"], solrServers[apiKey]["core"], req.URL.RawQuery)
        l4g.Debug("Proxying Request to %s for %s\n", solrUrl, apiKey)
        
        r, _, err := http.Get(solrUrl)
        if err != nil {
            //Set actual error page here
            l4g.Error("Error: %s\n", err.String())
            return
        }
        r.Write(w)
    }
    if req.Method == "POST" {
        //Handle indexing here. UserAuth? This will basically just set the post body into the queue
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
    
    server := fmt.Sprintf("%s:%s", config["host"], config["port"])
    
    if err := http.ListenAndServe(server, nil); err != nil {
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