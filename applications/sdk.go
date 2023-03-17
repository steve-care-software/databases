package databases

// Application represents the database application
type Application interface {
	Exists(name string) (bool, error)
	New(name string) error
	Delete(name string) error
	Open(name string) (*uint, error)
	Read(context uint, offset uint, length uint) ([]byte, error)
	Cancel(context uint) error
	Commit(context uint) error
	Close(context uint) error
}
