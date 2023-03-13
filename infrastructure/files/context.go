package files

import (
	"os"

	"github.com/juju/fslock"
	"github.com/steve-care-software/databases/domain/contents"
	"github.com/steve-care-software/databases/domain/references"
)

type context struct {
	identifier uint
	name       string
	pLock      *fslock.Lock
	pConn      *os.File
	reference  references.Reference
	dataOffset uint
	insertList []contents.Content
	delList    []references.ContentKey
}
