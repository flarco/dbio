package connection

import (
	"context"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/env"
	"github.com/flarco/g"
	"github.com/jedib0t/go-pretty/table"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

type ConnEntry struct {
	Name        string
	Description string
	Source      string
	Connection  Connection
}

var (
	localConns   []ConnEntry
	localConnsTs time.Time
)

func GetLocalConns(force ...bool) []ConnEntry {
	if len(force) > 0 && force[0] {
		// force refresh
	} else if time.Since(localConnsTs).Seconds() < 10 {
		return localConns // cachine to not re-read from disk. once every 10s
	}

	connsMap := map[string]ConnEntry{}

	// TODO: add local disk connection
	// conn, _ := connection.NewConnection("LOCAL_DISK", dbio.TypeFileLocal, g.M("url", "file://."))
	// c := Conn{
	// 	Name:        "LOCAL_DISK",
	// 	Description: dbio.TypeFileLocal.NameLong(),
	// 	Source:      "built-in",
	// 	Connection:  conn,
	// }
	// connsMap[c.Name] = c

	// get dbt connections
	dbtConns, err := ReadDbtConnections()
	if !g.LogError(err) {
		for _, conn := range dbtConns {
			c := ConnEntry{
				Name:        strings.ToUpper(conn.Info().Name),
				Description: conn.Type.NameLong(),
				Source:      "dbt profiles yaml",
				Connection:  conn,
			}
			c.Name = strings.ReplaceAll(c.Name, "/", "_")
			c.Connection.Name = strings.ReplaceAll(c.Connection.Name, "/", "_")
			connsMap[c.Name] = c
		}
	}

	for name, homeDir := range env.HomeDirs {
		envFilePath := env.GetEnvFilePath(homeDir)
		if g.PathExists(envFilePath) {
			m := g.M()
			g.JSONConvert(env.LoadEnvFile(envFilePath), &m)
			profileConns, err := ReadConnections(m)
			if !g.LogError(err) {
				for _, conn := range profileConns {
					c := ConnEntry{
						Name:        strings.ToUpper(conn.Info().Name),
						Description: conn.Type.NameLong(),
						Source:      name + " env yaml",
						Connection:  conn,
					}
					connsMap[c.Name] = c
				}
			}
		}

	}

	// Environment variables
	for key, val := range g.KVArrToMap(os.Environ()...) {
		if !strings.Contains(val, ":/") || strings.Contains(val, "{") {
			continue
		}

		key = strings.ToUpper(key)
		conn, err := NewConnectionFromURL(key, val)
		if err != nil {
			e := g.F("could not parse %s: %s", key, g.ErrMsgSimple(err))
			g.Warn(e)
			continue
		}

		if conn.Type.NameLong() == "" || conn.Info().Type == dbio.TypeUnknown || conn.Info().Type == dbio.TypeFileHTTP {
			continue
		}

		c := ConnEntry{
			Name:        conn.Info().Name,
			Description: conn.Type.NameLong(),
			Source:      "env variable",
			Connection:  conn,
		}
		if exC, ok := connsMap[c.Name]; ok {
			g.Warn(
				"conn credentials of %s from %s overwritten by env var %s",
				exC.Name, exC.Source, c.Name,
			)
		}
		connsMap[c.Name] = c
	}

	connArr := lo.Values(connsMap)
	sort.Slice(connArr, func(i, j int) bool {
		return cast.ToString(connArr[i].Name) < cast.ToString(connArr[j].Name)
	})

	localConnsTs = time.Now()
	localConns = connArr

	return connArr
}

type EnvConns struct {
	EnvFile *env.EnvFile
}

func (ec *EnvConns) Set(name string, kvMap map[string]any) (err error) {

	if name == "" {
		return g.Error("name is blank")
	}

	// parse url
	if url := cast.ToString(kvMap["url"]); url != "" {
		conn, err := NewConnectionFromURL(name, url)
		if err != nil {
			return g.Error(err, "could not parse url")
		}
		if _, ok := kvMap["type"]; !ok {
			kvMap["type"] = conn.Type.String()
		}
	}

	t, found := kvMap["type"]
	if _, typeOK := dbio.ValidateType(cast.ToString(t)); found && !typeOK {
		return g.Error("invalid type (%s)", cast.ToString(t))
	} else if !found {
		return g.Error("need to specify valid `type` key or provide `url`.")
	}

	ef := ec.EnvFile
	ef.Connections[name] = kvMap
	err = ef.WriteEnvFile()
	if err != nil {
		return g.Error(err, "could not write env file")
	}

	return
}

func (ec *EnvConns) Unset(name string) (err error) {
	if name == "" {
		return g.Error("name is blank")
	}

	ef := ec.EnvFile
	_, ok := ef.Connections[name]
	if !ok {
		return g.Error("did not find connection `%s`", name)
	}

	delete(ef.Connections, name)
	err = ef.WriteEnvFile()
	if err != nil {
		return g.Error(err, "could not write env file")
	}

	return
}

func (ec *EnvConns) List() string {
	conns := GetLocalConns(true)
	T := table.NewWriter()
	T.AppendHeader(table.Row{"Conn Name", "Conn Type", "Source"})
	for _, conn := range conns {
		T.AppendRow(table.Row{conn.Name, conn.Description, conn.Source})
	}
	return T.Render()
}

type DiscoverOptions struct {
	Filter   string
	Schema   string
	Folder   string
	discover bool
}

func (ec *EnvConns) Discover(name string, opt DiscoverOptions) (streamNames []string, err error) {
	opt.discover = true
	_, streamNames, err = ec.testDiscover(name, opt)
	return
}

func (ec *EnvConns) Test(name string) (ok bool, err error) {
	ok, _, err = ec.testDiscover(name, DiscoverOptions{})
	return
}

func (ec *EnvConns) testDiscover(name string, opt DiscoverOptions) (ok bool, streamNames []string, err error) {
	discover := opt.discover
	schema := opt.Schema
	folder := opt.Folder
	filter := opt.Filter

	conns := map[string]ConnEntry{}
	for _, conn := range GetLocalConns() {
		conns[strings.ToLower(conn.Name)] = conn
	}

	conn, ok1 := conns[strings.ToLower(name)]
	if !ok1 || name == "" {
		return ok, streamNames, g.Error("Invalid Connection name: %s", name)
	}

	switch {

	case conn.Connection.Type.IsDb():
		dbConn, err := conn.Connection.AsDatabase()
		if err != nil {
			return ok, streamNames, g.Error(err, "could not initiate %s", name)
		}
		err = dbConn.Connect()
		if err != nil {
			return ok, streamNames, g.Error(err, "could not connect to %s", name)
		}
		if discover {
			schemata, err := dbConn.GetSchemata(schema, "")
			if err != nil {
				return ok, streamNames, g.Error(err, "could not discover %s", name)
			}
			for _, table := range schemata.Tables() {
				streamNames = append(streamNames, table.FullName())
			}
		}

	case conn.Connection.Type.IsFile():
		fileClient, err := conn.Connection.AsFile()
		if err != nil {
			return ok, streamNames, g.Error(err, "could not initiate %s", name)
		}
		err = fileClient.Init(context.Background())
		if err != nil {
			return ok, streamNames, g.Error(err, "could not connect to %s", name)
		}

		url := conn.Connection.URL()
		if folder != "" {
			if !strings.HasPrefix(folder, string(fileClient.FsType())+"://") {
				return ok, streamNames, g.Error("need to use proper URL for folder path. Example -> %s/my-folder", url)
			}
			url = folder
		}

		streamNames, err = fileClient.List(url)
		if err != nil {
			return ok, streamNames, g.Error(err, "could not connect to %s", name)
		}

	case conn.Connection.Type.IsAirbyte():
		client, err := conn.Connection.AsAirbyte()
		if err != nil {
			return ok, streamNames, g.Error(err, "could not initiate %s", name)
		}
		err = client.Init(!discover)
		if err != nil {
			return ok, streamNames, g.Error(err, "could not connect to %s", name)
		}
		if discover {
			streams, err := client.Discover()
			if err != nil {
				return ok, streamNames, g.Error(err, "could not discover %s", name)
			}
			streamNames = streams.Names()
		}

	default:
		return ok, streamNames, g.Error("Unhandled connection type: %s", conn.Connection.Type)
	}

	if discover {
		// sort alphabetically
		sort.Slice(streamNames, func(i, j int) bool {
			return streamNames[i] < streamNames[j]
		})

		filters := strings.Split(filter, ",")
		streamNames = lo.Filter(streamNames, func(n string, i int) bool {
			return filter == "" || g.IsMatched(filters, n)
		})
		if len(streamNames) > 0 && conn.Connection.Type.IsFile() &&
			folder == "" {
			g.Warn("Those are non-recursive folder or file names (at the root level). Please use --folder flag to list sub-folders")
			println()
		}
	} else {
		ok = true
	}

	return
}
