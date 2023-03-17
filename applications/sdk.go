package databases

import (
	"github.com/steve-care-software/databases/domain/references"
	"github.com/steve-care-software/libs/cryptography/hash"
)

// Application represents the database application
type Application interface {
	Exists(name string) (bool, error)
	New(name string) error
	Delete(name string) error
	Open(name string) (*uint, error)
	ContentKeys(context uint, kind uint) (references.ContentKeys, error)
	Commits(context uint) (references.Commits, error)
	Read(context uint, pointer references.Pointer) ([]byte, error)
	ReadAll(context uint, pointers []references.Pointer) ([][]byte, error)
	Write(context uint, kind uint, hash hash.Hash, data []byte) error
	Erase(context uint, contentKey references.ContentKey) error
	Cancel(context uint) error
	Commit(context uint) error
	Close(context uint) error
}
