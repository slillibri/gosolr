//This is a monolithic proxy I am writing for a solr-as-service thing I am working on
//Probably unnecessary but it's fun learning new languages.
//Note, probably nothing in this is the Go way

package main

import (
    "mysql"
    "flag"
    "conf"
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
        l4g.Debug("Proxying Request to %s for %s", solrUrl, apiKey)
        // fmt.Printf("Proxying Request to %s for %s\n", solrUrl, apiKey)
        
        r, _, err := http.Get(solrUrl)
        if err != nil {
            //Set actual error page here
            fmt.Fprintf(w, "Error: %s", err.String())
            return
        }
        r.Write(w)
    }
    if req.Method == "POST" {
        //Handle indexing here. UserAuth? This will basically just set the post body into the queue
    }
    
}

func main() {
    //Load default config file
    var configPath string
    flag.StringVar(&configPath, "config", "/etc/gosolr/gosolr.cfg", "Path to the configuration file")
    flag.Parse()
    config = loadConfig(configPath)
    
    //Load solr servers/cores from mysql db
    solrServers = make(map[string]map[string]string)
    loadSolrServers(config)
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

//Move these into a different package?
func loadConfig(file string) (map[string]string) {
    config, err := conf.ReadConfigFile(file)
    if err != nil {
        l4g.Error("Error reading config: %s", err.String())
        os.Exit(1)
    }
    
    //Setup configuration values
    values := make(map[string]string)
    keys := []string{"host", "port", "db_host", "db_port", "db_user", "db_pass", "db_name"}
    for i := 0; i < len(keys); i++ {
        values[keys[i]] = getValue(config, keys[i])
    }
    
    return values
}

func getValue(config *conf.ConfigFile, key string) string {
    // I am a retarded function to save typeing...
    str, err := config.GetString("", key)
    if err != nil {
        //Exit if we can't find an expected value (these are all in the default namespace)
        l4g.Error("Error getting %s: %s", key, err.String())
        os.Exit(1)
    }
    return str
}

//Pull information from MySQL to find out which API keys we should handle and how they map
//  i.e. servers and cores
func loadSolrServers(config map[string]string) {
    db := mysql.New()
    l4g.Debug("db_host: %s", config["db_host"])
    if err := db.Connect(config["db_host"], config["db_user"], config["db_pass"], config["db_name"]); err != nil {
        l4g.Error("Error connecting to db: %s\n%s", err.String(), db.Error)
        os.Exit(1)
    }
    stmt, err := db.InitStmt()
    if err != nil {
        l4g.Error("Error initializing stmt: %s", err.String())
        os.Exit(1)
    }
    
    defer stmt.Close()
    stmt.Prepare("Select apistring,core,server from cores where gosolr = ?")
    stmt.BindParams(config["host"])
    
    res, err := stmt.Execute()
    if err != nil { 
        l4g.Error("error executing stmt: %s", err.String())
    }    
    
    solrServers = make(map[string]map[string]string)
    var row map[string]interface{}
    
    for {
        if row = res.FetchMap(); row == nil {
            //Read the last row
            break
        }
        //This cannot possibly be right...
        apistring := fmt.Sprintf("%s", row["apistring"])
        core := fmt.Sprintf("%s", row["core"])
        server := fmt.Sprintf("%s", row["server"])
        solrServers[apistring] = map[string]string{"core":core, "server":server}
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