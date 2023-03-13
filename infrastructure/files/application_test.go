package files

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	"github.com/steve-care-software/libs/cryptography/hash"
)

func TestExists_thenCreate_thenDelete_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	application := NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)

	name := "my_name"
	exists, err := application.Exists(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if exists {
		t.Errorf("the database was expected to NOT exists")
		return
	}

	err = application.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	exists, err = application.Exists(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if !exists {
		t.Errorf("the database was expected to exists")
		return
	}

	err = application.New(name)
	if err == nil {
		t.Errorf("the error was expected to be valid, nil returned")
		return
	}

	err = application.Delete(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	exists, err = application.Exists(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if exists {
		t.Errorf("the database was expected to NOT exists")
		return
	}
}

func TestCreate_thenOpen_thenWrite_thenRead_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	hashAdapter := hash.NewAdapter()
	application := NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)

	name := "my_name"
	err := application.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pContext, err := application.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	data := []byte("this is some data")
	pHash, err := hashAdapter.FromBytes(data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	kind := uint(0)
	err = application.Write(*pContext, kind, *pHash, data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retData, err := application.ReadByHash(*pContext, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(retData, data) != 0 {
		t.Errorf("the returned data is invalid")
		return
	}

	retContentKeys, err := application.ContentKeysByKind(*pContext, kind)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retContentKeysList := retContentKeys.List()
	if len(retContentKeysList) != 1 {
		t.Errorf("%d contentKeys od kinf (%d) were expected, %d returned", kind, 1, len(retContentKeysList))
		return
	}

	invalidKind := uint(2345234)
	_, err = application.ContentKeysByKind(*pContext, invalidKind)
	if err == nil {
		t.Errorf("the error was expected to be valid, nil returned")
		return
	}

	retCommits, err := application.Commits(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	list := retCommits.List()
	if len(list) != 1 {
		t.Errorf("%d commits were expected, %d returned", 1, len(list))
		return
	}

	retCommit, err := application.CommitByHash(*pContext, list[0].Hash())
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if !reflect.DeepEqual(retCommit, list[0]) {
		t.Errorf("the returned commit is invalid")
		return
	}

	// erase by hashes:
	err = application.EraseAllByHashes(*pContext, []hash.Hash{
		*pHash,
	})

	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// commit:
	err = application.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// read again, returns an error:
	_, err = application.ReadByHash(*pContext, *pHash)
	if err == nil {
		t.Errorf("the error was expected to be valid, nil returned")
		return
	}

	// insert again the resource we just deleted:
	err = application.Write(*pContext, kind, *pHash, data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Close(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pSecondContext, err := application.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	secondData := []byte("this is some second additional data")
	pSecondHash, err := hashAdapter.FromBytes(secondData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Write(*pSecondContext, kind, *pSecondHash, secondData)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Commit(*pSecondContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retSecondData, err := application.ReadByHash(*pSecondContext, *pSecondHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Close(*pSecondContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(retSecondData, secondData) != 0 {
		t.Errorf("the returned data is invalid")
		return
	}

	pThirdContext, err := application.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	retFirstData, err := application.ReadByHash(*pThirdContext, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	if bytes.Compare(retFirstData, data) != 0 {
		t.Errorf("the returned data is invalid")
		return
	}

	err = application.Close(*pThirdContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}
}

func TestCreate_New_Insert_Erase_Success(t *testing.T) {
	dirPath := "./test_files"
	dstExtension := "destination"
	bckExtension := "backup"
	readChunkSize := uint(1000000)
	defer func() {
		os.RemoveAll(dirPath)
	}()

	name := "my_name"
	application := NewApplication(dirPath, dstExtension, bckExtension, readChunkSize)
	err := application.New(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	pContext, err := application.Open(name)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	defer application.Close(*pContext)
	data := []byte("this is some data")
	pHash, err := hash.NewAdapter().FromBytes(data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	kind := uint(0)
	err = application.Write(*pContext, kind, *pHash, data)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	// erase:
	err = application.EraseByHash(*pContext, *pHash)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}

	err = application.Commit(*pContext)
	if err != nil {
		t.Errorf("the error was expected to be nil, error returned: %s", err.Error())
		return
	}
}
