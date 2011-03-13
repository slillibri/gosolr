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
func loadConfig(file string) (map[string]map[string]string) {
    config, err := conf.ReadConfigFile(file)
    if err != nil {
        l4g.Error("error reading config: %s\n", err.String())
        os.Exit(1)
    }
    
    //Setup configuration values
    values := make(map[string]map[string]string)
    
    //Setup some default values - these are in the default namespace
    //There are 3 configuration namespaces default, database, and stomp
    values["default"] = map[string]string{"host":"localhost", "port":"80", "read_timeout":"0", "write_timeout":"0"}
    l4g.Debug("Default vals: %v", values)
    
    //TODO move stuff out of the default namespace
    //Fetch default keys
    keys := []string{"host", "port", "read_timeout", "write_timeout"}
    fetchKeys(keys, "default", config, values)
    
    db_keys := []string{"host", "port", "user", "pass", "name"}
    fetchKeys(db_keys, "database", config, values)
    
    stomp_keys := []string{"host"}
    fetchKeys(stomp_keys, "stomp", config, values)
    
    l4g.Debug("Final values: %v", values)
    return values
}

func fetchKeys(keys []string, namespace string, config *conf.ConfigFile, values map[string]map[string]string) {
    for i := 0; i < len(keys); i++ {
        //Test if namespace exists, otherwise create it
        if _, ns := values[namespace]; !ns {
            l4g.Debug("Creating map for namespace %s", namespace)
            values[namespace] = make(map[string]string)
        }
        //If there is a default value it's ok if the config key doesn't exist
        _, ok := values[namespace][keys[i]]
        if ok {
            set := getValue(config, keys[i], namespace, false)
            if set != "" {
                values[namespace][keys[i]] = set
            }
        } else {
            values[namespace][keys[i]] = getValue(config, keys[i], namespace, true)
        }
    }    
}

func getValue(config *conf.ConfigFile, key string, namespace string, fail bool) string {
    // I am a retarded function to save typeing...    
    str, err := config.GetString(namespace, key)
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
func loadSolrServers(config map[string]map[string]string) (map[string]map[string]string){
    db, err := mysql.DialTCP(config["database"]["host"], config["database"]["user"], config["database"]["pass"], config["database"]["name"])
    if err != nil {
        l4g.Error("Error connecting to db: %s\n", err.String())
        os.Exit(1)
    }
    stmt, err := db.Prepare("Select apistring,core,server from cores where gosolr = ?")
    if err != nil {
        l4g.Error("Error preparing statement: %s", err.String())
        os.Exit(1)
    }
    
    stmt.BindParams(config["default"]["host"])
    
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