package main

import(
    "mysql"
    "conf"
    "fmt"
    "os"
)

type SolrData struct {
    apistring   string
    core        string
    server      string
}

//Move these into a different package?
func loadConfig(file string) (map[string]string) {
    config, err := conf.ReadConfigFile(file)
    if err != nil {
        fmt.Printf("error reading config: %s\n", err.String())
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
        fmt.Printf("Error getting %s: %s\n", key, err.String())
        os.Exit(1)
    }
    return str
}

//Pull information from MySQL to find out which API keys we should handle and how they map
//  i.e. servers and cores
func loadSolrServers(config map[string]string) (map[string]map[string]string){
    fmt.Printf("db_host: %s\n", config["db_host"])
    db, err := mysql.DialTCP(config["db_host"], config["db_user"], config["db_pass"], config["db_name"])
    if err != nil {
        fmt.Printf("Error connecting to db: %s\n", err.String())
        os.Exit(1)
    }
    stmt, err := db.Prepare("Select apistring,core,server from cores where gosolr = ?")
    if err != nil {
        fmt.Printf("Error preparing statement: %s", err.String())
        os.Exit(1)
    }
    
    stmt.BindParams(config["host"])
    
    err = stmt.Execute()
    if err != nil { 
        fmt.Printf("error executing stmt: %s", err.String())
    }    
    
    var solrrow SolrData
    stmt.BindResult(&solrrow.apistring, &solrrow.core, &solrrow.server)
    solr_values := make(map[string]map[string]string)
    
    for {
        eof, err := stmt.Fetch()
        if err != nil {
            fmt.Printf("Error fetching row: %s", err.String())
        }
        
        solr_values[solrrow.apistring] = map[string]string{"core":solrrow.core, "server":solrrow.server}
        if eof {
            break
        }
    }
    return solr_values
}
