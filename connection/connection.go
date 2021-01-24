package connection

type Kind string

const (
	// KindDatabase for databases
	KindDatabase Kind = "database"
	// KindFile for files (cloud, sftp)
	KindFile Kind = "file"
	// KindAPI for APIs
	KindAPI Kind = "api"
)

// Info is the connection type
type Info struct {
	Name string
	Type string
	Kind Kind
}

// Connection is a connection
type Connection interface {
	Self() Connection
	Init() error
	Connect(timeOut ...int) error
	Close() error
	Info() Info
	ToMap() map[string]interface{}
}
