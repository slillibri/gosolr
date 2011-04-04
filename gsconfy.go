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
    authstring  string
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
    //If the default value is "" that means it is required in the config file.
    //An empty value in the config will not overwrite the default value
    values["default"] = map[string]string{"host":"localhost", "port":"80", "read_timeout":"0", "write_timeout":"0"}
    values["database"] = map[string]string{"host":"localhost", "port":"3306", "user":"", "pass":"", "name":""}
    values["stomp"] = map[string]string{"host":""}

    l4g.Debug("Default vals: %v", values)
    
    //Read values from config
    for namespace := range values {
        //If there is a default value it's ok if the config key doesn't exist
        for key, value := range values[namespace] {
            if value != "" {
                set := getValue(config, key, namespace, false)
                if set != "" {
                    values[namespace][key] = set
                }
            } else {
                values[namespace][key] = getValue(config, key, namespace, true)
            }
        }
    }

    l4g.Debug("Final values: %v", values)
    return values
}

func getValue(config *conf.ConfigFile, key string, namespace string, fail bool) string {
    str, err := config.GetString(namespace, key)
    if err != nil && fail {
        //Exit if we can't find an expected value
        l4g.Error("Error getting %s: %s\n", key, err.String())
        os.Exit(1)
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
    stmt, err := db.Prepare("Select apistring,core,server,authstring from cores where gosolr = ?")
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
    stmt.BindResult(&solrrow.apistring, &solrrow.core, &solrrow.server, &solrrow.authstring)
    solr_values := make(map[string]map[string]string)
    
    for {
        eof, err := stmt.Fetch()
        if err != nil {
            l4g.Error("Error fetching row: %s", err.String())
            os.Exit(1)
        }
        
        solr_values[solrrow.apistring] = map[string]string{"core":solrrow.core, "server":solrrow.server, "authstring":solrrow.authstring}
        if eof {
            break
        }
    }
    return solr_values
}