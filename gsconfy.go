package main

import(
    "mysql"
    "conf"
    "os"
    l4g "log4go.googlecode.com/hg"
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
        l4g.Error("error reading config: %s\n", err.String())
        os.Exit(1)
    }
    
    //Setup configuration values
    values := make(map[string]string)
    //Setup some default values
    values["host"] = "localhost"
    values["port"] = "80"
    values["read_timeout"] = "0"
    values["write_timeout"] = "0"

    keys := []string{"host", "port", "db_host", "db_port", "db_user", "db_pass", "db_name", "read_timeout", "write_timeout"}
    for i := 0; i < len(keys); i++ {
        if _, ok := values[keys[i]]; ok {
            set := getValue(config, keys[i], false)
            if set != "" {
                values[keys[i]] = set
            }
        } else {
            values[keys[i]] = getValue(config, keys[i], true)
        }
    }
    
    return values
}

func getValue(config *conf.ConfigFile, key string, fail bool) string {
    // I am a retarded function to save typeing...    
    str, err := config.GetString("", key)
    if err != nil {
        if fail {
	        //Exit if we can't find an expected value (these are all in the default namespace)
            l4g.Error("Error getting %s: %s\n", key, err.String())
            os.Exit(1)
        }
    }
    return str
}

//Pull information from MySQL to find out which API keys we should handle and how they map
//  i.e. servers and cores
func loadSolrServers(config map[string]string) (map[string]map[string]string){
    l4g.Debug("db_host: %s\n", config["db_host"])
    db, err := mysql.DialTCP(config["db_host"], config["db_user"], config["db_pass"], config["db_name"])
    if err != nil {
        l4g.Error("Error connecting to db: %s\n", err.String())
        os.Exit(1)
    }
    stmt, err := db.Prepare("Select apistring,core,server from cores where gosolr = ?")
    if err != nil {
        l4g.Error("Error preparing statement: %s", err.String())
        os.Exit(1)
    }
    
    stmt.BindParams(config["host"])
    
    err = stmt.Execute()
    if err != nil { 
        l4g.Error("error executing stmt: %s", err.String())
        os.Exit(1)
    }    
    
    var solrrow SolrData
    stmt.BindResult(&solrrow.apistring, &solrrow.core, &solrrow.server)
    solr_values := make(map[string]map[string]string)
    
    for {
        eof, err := stmt.Fetch()
        if err != nil {
            l4g.Error("Error fetching row: %s", err.String())
            os.Exit(1)
        }
        
        solr_values[solrrow.apistring] = map[string]string{"core":solrrow.core, "server":solrrow.server}
        if eof {
            break
        }
    }
    return solr_values
}