package connection

import (
	"os"
	"sort"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/env"
	"github.com/flarco/g"
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
