package files

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/juju/fslock"
	databases "github.com/steve-care-software/databases/applications"
	"github.com/steve-care-software/databases/domain/contents"
	"github.com/steve-care-software/databases/domain/references"
	"github.com/steve-care-software/libs/cryptography/trees"
)

type application struct {
	onOpenFn                    databases.OnOpenFn
	contentsBuilder             contents.Builder
	contentBuilder              contents.ContentBuilder
	referenceAdapter            references.Adapter
	referenceBuilder            references.Builder
	referenceContentKeysBuilder references.ContentKeysBuilder
	referenceContentKeyBuilder  references.ContentKeyBuilder
	referenceCommitsBuilder     references.CommitsBuilder
	referenceCommitAdapter      references.CommitAdapter
	referenceCommitBuilder      references.CommitBuilder
	referenceActionBuilder      references.ActionBuilder
	referencePointerBuilder     references.PointerBuilder
	hashTreeBuilder             trees.Builder
	dirPath                     string
	dstExtension                string
	bckExtension                string
	readChunkSize               uint
	contexts                    map[uint]*context
}

func createApplication(
	onOpenFn databases.OnOpenFn,
	contentsBuilder contents.Builder,
	contentBuilder contents.ContentBuilder,
	referenceAdapter references.Adapter,
	referenceBuilder references.Builder,
	referenceContentKeysBuilder references.ContentKeysBuilder,
	referenceContentKeyBuilder references.ContentKeyBuilder,
	referenceCommitsBuilder references.CommitsBuilder,
	referenceCommitAdapter references.CommitAdapter,
	referenceCommitBuilder references.CommitBuilder,
	referenceActionBuilder references.ActionBuilder,
	referencePointerBuilder references.PointerBuilder,
	hashTreeBuilder trees.Builder,
	dirPath string,
	dstExtension string,
	bckExtension string,
	readChunkSize uint,
) databases.Application {
	out := application{
		onOpenFn:                    onOpenFn,
		contentsBuilder:             contentsBuilder,
		contentBuilder:              contentBuilder,
		referenceAdapter:            referenceAdapter,
		referenceBuilder:            referenceBuilder,
		referenceContentKeysBuilder: referenceContentKeysBuilder,
		referenceContentKeyBuilder:  referenceContentKeyBuilder,
		referenceCommitsBuilder:     referenceCommitsBuilder,
		referenceCommitAdapter:      referenceCommitAdapter,
		referenceCommitBuilder:      referenceCommitBuilder,
		referenceActionBuilder:      referenceActionBuilder,
		referencePointerBuilder:     referencePointerBuilder,
		hashTreeBuilder:             hashTreeBuilder,
		dirPath:                     dirPath,
		dstExtension:                dstExtension,
		bckExtension:                bckExtension,
		readChunkSize:               readChunkSize,
		contexts:                    map[uint]*context{},
	}

	return &out
}

// Exists returns true if the database exists, false otherwise
func (app *application) Exists(name string) (bool, error) {
	path := filepath.Join(app.dirPath, name)
	fileInfo, err := os.Stat(path)
	if err == nil {
		return !fileInfo.IsDir(), nil
	}

	return false, nil
}

// New creates a new database
func (app *application) New(name string) error {
	if _, err := os.Stat(app.dirPath); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(app.dirPath, filePermission)
		if err != nil {
			return err
		}
	}

	path := filepath.Join(app.dirPath, name)
	_, err := os.Stat(path)
	if err == nil {
		str := fmt.Sprintf("the database (name: %s) already exists and therefore cannot be created again", name)
		return errors.New(str)
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	return file.Close()
}

// Delete deletes an existing database
func (app *application) Delete(name string) error {
	path := filepath.Join(app.dirPath, name)
	pInfo, err := os.Stat(path)
	if err != nil {
		return err
	}

	if pInfo.IsDir() {
		str := fmt.Sprintf("the name (%s) was expected to be a file, not a directory", name)
		return errors.New(str)
	}

	return os.Remove(path)
}

// Open opens a context on a given database
func (app *application) Open(name string) (*uint, error) {
	for _, oneContext := range app.contexts {
		if oneContext.name == name {
			str := fmt.Sprintf("there is already an open context for the provided name: %s", name)
			return nil, errors.New(str)
		}
	}

	// execute the open callback
	err := app.onOpenFn(name)
	if err != nil {
		return nil, err
	}

	// open the connection:
	path := filepath.Join(app.dirPath, name)
	pConn, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// create a Lock instance on the path:
	pLock := fslock.New(path)

	// create the context:
	pContext := &context{
		identifier: uint(len(app.contexts)),
		pConn:      pConn,
		pLock:      pLock,
		name:       name,
		insertList: []contents.Content{},
		delList:    map[string]references.ContentKey{},
	}

	app.contexts[pContext.identifier] = pContext
	return &pContext.identifier, nil
}

// Lock locks the database file using the provided context
func (app *application) Lock(context uint) error {
	if pContext, ok := app.contexts[context]; ok {
		return pContext.pLock.TryLock()
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot Lock using this context", context)
	return errors.New(str)
}

// Unlock unlocks the database file using the provided context
func (app *application) Unlock(context uint) error {
	if pContext, ok := app.contexts[context]; ok {
		return pContext.pLock.Unlock()
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot Unlock using this context", context)
	return errors.New(str)
}

// Read reads data using context, at offset, for a given length
func (app *application) Read(context uint, offset uint, length uint) ([]byte, error) {
	if pContext, ok := app.contexts[context]; ok {
		contentBytes := make([]byte, length)
		refContentAmount, err := pContext.pConn.ReadAt(contentBytes, int64(offset))
		if err != nil {
			return nil, err
		}

		if refContentAmount != int(length) {
			str := fmt.Sprintf("the Read operation was expected to read %d bytes, %d returned", length, refContentAmount)
			return nil, errors.New(str)
		}

		return contentBytes, nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot Read using this context", context)
	return nil, errors.New(str)
}

// Write writes data using context, at offset
func (app *application) Write(context uint, offset int64, data []byte) error {
	if pContext, ok := app.contexts[context]; ok {
		// seek the file at the from byte:
		seekOffset, err := pContext.pConn.Seek(offset, 0)
		if err != nil {
			return err
		}

		if seekOffset != offset {
			str := fmt.Sprintf("the offset was expected to be %d, %d returned after file seek", offset, seekOffset)
			return errors.New(str)
		}

		// write the data on disk:
		amountWritten, err := pContext.pConn.Write(data)
		if err != nil {
			return err
		}

		amountExpected := len(data)
		if amountExpected != amountWritten {
			str := fmt.Sprintf("%d bytes were expected to be written, %d actually written", amountExpected, amountWritten)
			return errors.New(str)
		}

		return nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot Write using this context", context)
	return errors.New(str)
}

// Close closes a context
func (app *application) Close(context uint) error {
	if pContext, ok := app.contexts[context]; ok {
		err := pContext.pConn.Close()
		if err != nil {
			return err
		}

		delete(app.contexts, context)
		return nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot be closed", context)
	return errors.New(str)
}
