package files

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/juju/fslock"
	databases "github.com/steve-care-software/databases/applications"
	"github.com/steve-care-software/databases/domain/contents"
	"github.com/steve-care-software/databases/domain/references"
	"github.com/steve-care-software/libs/cryptography/hash"
	"github.com/steve-care-software/libs/cryptography/trees"
)

type application struct {
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

	reference, offset, err := app.retrieveReference(name)
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
		reference:  reference,
		dataOffset: offset,
		insertList: []contents.Content{},
		delList:    map[string]references.ContentKey{},
	}

	app.contexts[pContext.identifier] = pContext
	return &pContext.identifier, nil
}

func (app *application) makeChunkSize(length uint) uint {
	if app.readChunkSize > length {
		return length
	}

	return app.readChunkSize
}

func (app *application) retrieveReference(name string) (references.Reference, uint, error) {
	path := filepath.Join(app.dirPath, name)
	pConn, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	defer pConn.Close()

	// read the reference length in bytes:
	refLengthBytes := make([]byte, expectedReferenceBytesLength)
	refAmount, err := pConn.ReadAt(refLengthBytes, 0)
	if err != nil || refAmount <= 0 {
		return nil, 0, nil
	}

	if refAmount != expectedReferenceBytesLength {
		str := fmt.Sprintf("%d bytes were expected to be read when reading the reference length bytes, %d actually read", expectedReferenceBytesLength, refAmount)
		return nil, 0, errors.New(str)
	}

	// convert the reference length to uint64:
	refLength := int(binary.LittleEndian.Uint64(refLengthBytes))

	// read the reference data:
	refAllBytes := []byte{}
	offset := int64(expectedReferenceBytesLength)

	// setup the read chunk size:
	chunkSize := int(app.makeChunkSize(uint(refLength)))
	amount := int((refLength / chunkSize) + 1)
	lastChunkSize := refLength - (chunkSize * (amount - 1))

	for i := 0; i < amount; i++ {
		readSize := chunkSize
		if i+1 >= amount {
			readSize = lastChunkSize
		}

		refContentBytes := make([]byte, readSize)
		refContentAmount, err := pConn.ReadAt(refContentBytes, offset)
		if err != nil {
			return nil, 0, err
		}

		refAllBytes = append(refAllBytes, refContentBytes...)
		offset += int64(refContentAmount)
	}

	// convert the content to a reference instance:
	ins, err := app.referenceAdapter.ToReference(refAllBytes)
	if err != nil {
		return nil, 0, err
	}

	return ins, uint(offset), nil
}

// ContentKeys returns the contentKeys by context and kind
func (app *application) ContentKeys(context uint, kind uint) (references.ContentKeys, error) {
	contentKeys, err := app.contentKeys(context)
	if err != nil {
		return nil, err
	}

	if contentKeys == nil {
		return nil, errors.New("there is no content in the database")
	}

	list, err := contentKeys.ListByKind(kind)
	if err != nil {
		return nil, err
	}

	return app.referenceContentKeysBuilder.Create().WithList(list).Now()
}

func (app *application) contentKeys(context uint) (references.ContentKeys, error) {
	if pContext, ok := app.contexts[context]; ok {
		if pContext.reference == nil {
			str := fmt.Sprintf("there is zero (0) ContentKey in the given context: %d", context)
			return nil, errors.New(str)
		}

		return pContext.reference.ContentKeys(), nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot return the Content instance", context)
	return nil, errors.New(str)
}

// Commits returns the commits on a context
func (app *application) Commits(context uint) (references.Commits, error) {
	if pContext, ok := app.contexts[context]; ok {
		if pContext.reference == nil {
			str := fmt.Sprintf("there is zero (0) Commit in the given context: %d", context)
			return nil, errors.New(str)
		}

		return pContext.reference.Commits(), nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot return the Commits instance", context)
	return nil, errors.New(str)
}

// Read reads a pointer on a context
func (app *application) Read(context uint, pointer references.Pointer) ([]byte, error) {
	if pContext, ok := app.contexts[context]; ok {
		offset := pContext.dataOffset + pointer.From()
		length := pointer.Length()
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

// ReadAll read pointers on a context
func (app *application) ReadAll(context uint, pointers []references.Pointer) ([][]byte, error) {
	output := [][]byte{}
	for _, onePointer := range pointers {
		content, err := app.Read(context, onePointer)
		if err != nil {
			return nil, err
		}

		output = append(output, content)
	}

	return output, nil
}

// Write writes data to a context
func (app *application) Write(context uint, kind uint, hash hash.Hash, data []byte) error {
	if pContext, ok := app.contexts[context]; ok {
		contentIns, err := app.contentBuilder.Create().WithHash(hash).WithData(data).WithKind(kind).Now()
		if err != nil {
			return err
		}

		pContext.insertList = append(pContext.insertList, contentIns)
		app.contexts[context] = pContext
		return nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot be written to", context)
	return errors.New(str)
}

func (app *application) makeToDeleteKeyname(kind uint, hash hash.Hash) string {
	return fmt.Sprintf("%d%s", kind, hash.String())
}

// Erase erases a contentKey
func (app *application) Erase(context uint, contentKey references.ContentKey) error {
	if _, ok := app.contexts[context]; !ok {
		str := fmt.Sprintf("the given context (%d) does not exists and therefore the resource cannot be deleted by hash", context)
		return errors.New(str)
	}

	hash := contentKey.Hash()
	kind := contentKey.Kind()
	keyname := fmt.Sprintf("%d%s", kind, hash.String())
	app.contexts[context].delList[keyname] = contentKey
	return nil
}

// Cancel cancels a context
func (app *application) Cancel(context uint) error {
	if pContext, ok := app.contexts[context]; ok {
		app.contexts[context] = pContext
		return nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot be canceled", context)
	return errors.New(str)
}

// Commit commits a context
func (app *application) Commit(context uint) error {
	// update the reference:
	updatedReference, err := app.updateReference(context)
	if err != nil {
		return err
	}

	if updatedReference == nil {
		return nil
	}

	if pContext, ok := app.contexts[context]; ok {
		// update database on disk:
		pConn, pDataOffset, err := app.updateDatabaseOnFile(pContext, updatedReference)
		if err != nil {
			return err
		}

		// update the file connection and reference:
		app.contexts[context].reference = updatedReference
		app.contexts[context].dataOffset = *pDataOffset
		app.contexts[context].pConn = pConn
		app.contexts[context].insertList = []contents.Content{}
		app.contexts[context].delList = map[string]references.ContentKey{}
		return nil
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot be comitted", context)
	return errors.New(str)
}

func (app *application) updateReference(context uint) (references.Reference, error) {
	if pContext, ok := app.contexts[context]; ok {
		// retrieve the commits list:
		commitsList := []references.Commit{}
		if pContext.reference != nil {
			commitsList = pContext.reference.Commits().List()
		}

		// find the offset:
		offset := int64(0)
		if pContext.reference != nil {
			if pContext.reference.HasContentKeys() {
				offset = pContext.reference.ContentKeys().Next()
			}
		}

		// find the latest commit:
		builder := app.referenceCommitBuilder.Create()
		if pContext.reference != nil {
			refCommit := pContext.reference.Commits().Latest()
			builder.WithParent(refCommit.Hash())
		}

		// get the pending content list:
		contentKeysMap := map[string]references.ContentKey{}
		if pContext.reference != nil {
			if pContext.reference.HasContentKeys() {
				contentKeysList := pContext.reference.ContentKeys().List()
				for _, oneContentKey := range contentKeysList {
					keyname := oneContentKey.Hash().String()
					contentKeysMap[keyname] = oneContentKey
				}
			}
		}

		// if there is content to delete:
		if len(pContext.delList) > 0 {
			blocks := [][]byte{}
			for _, oneContentKey := range pContext.delList {
				// add the hash in the blocks for the commit values:
				blocks = append(blocks, oneContentKey.Hash().Bytes())
			}

			values, err := app.hashTreeBuilder.Create().WithBlocks(blocks).Now()
			if err != nil {
				return nil, err
			}

			action, err := app.referenceActionBuilder.Create().WithDelete(values).Now()
			if err != nil {
				return nil, err
			}

			createdOn := time.Now().UTC()
			commit, err := builder.WithAction(action).CreatedOn(createdOn).Now()
			if err != nil {
				return nil, err
			}

			for _, oneContentKey := range pContext.delList {
				// update the offset:
				offset -= int64(oneContentKey.Content().Length())

				// remove the content key:
				keyname := oneContentKey.Hash().String()
				if _, ok := contentKeysMap[keyname]; ok {
					delete(contentKeysMap, keyname)
				}
			}

			// save the commit in the list:
			commitsList = append(commitsList, commit)
		}

		contentKeysList := []references.ContentKey{}
		for _, oneContentKey := range contentKeysMap {
			contentKeysList = append(contentKeysList, oneContentKey)
		}

		// if there is content to insert:
		if len(pContext.insertList) > 0 {
			blocks := [][]byte{}
			for _, oneContent := range pContext.insertList {
				// add the hash in the blocks for the commit values:
				blocks = append(blocks, oneContent.Hash().Bytes())
			}

			values, err := app.hashTreeBuilder.Create().WithBlocks(blocks).Now()
			if err != nil {
				return nil, err
			}

			action, err := app.referenceActionBuilder.Create().WithInsert(values).Now()
			if err != nil {
				return nil, err
			}

			createdOn := time.Now().UTC()
			commit, err := builder.WithAction(action).CreatedOn(createdOn).Now()
			if err != nil {
				return nil, err
			}

			commitHash := commit.Hash()
			for _, oneContent := range pContext.insertList {
				// build the pointer:
				dataLength := int64(len(oneContent.Data()))
				contentKeyPointer, err := app.referencePointerBuilder.Create().From(uint(offset)).WithLength(uint(dataLength)).Now()
				if err != nil {
					return nil, err
				}

				// build the content key:
				contentKey, err := app.referenceContentKeyBuilder.Create().WithHash(oneContent.Hash()).WithKind(oneContent.Kind()).WithContent(contentKeyPointer).WithCommit(commitHash).Now()
				if err != nil {
					return nil, err
				}

				//save the content key to the list:
				contentKeysList = append(contentKeysList, contentKey)

				// update the offset:
				offset += dataLength
			}

			// save the commit in the list:
			commitsList = append(commitsList, commit)
		}

		commits, err := app.referenceCommitsBuilder.Create().WithList(commitsList).Now()
		if err != nil {
			return nil, err
		}

		referenceBuilder := app.referenceBuilder.Create().WithCommits(commits)
		if len(contentKeysList) > 0 {
			updatedContentKeys, err := app.referenceContentKeysBuilder.Create().WithList(contentKeysList).Now()
			if err != nil {
				return nil, err
			}

			referenceBuilder.WithContentKeys(updatedContentKeys)
		}

		return referenceBuilder.Now()
	}

	str := fmt.Sprintf("the given context (%d) does not exists and therefore cannot be comitted", context)
	return nil, errors.New(str)
}

func (app *application) updateDatabaseOnFile(context *context, updatedReference references.Reference) (*os.File, *uint, error) {
	// create a lock on the file:
	err := context.pLock.TryLock()
	if err != nil {
		return nil, nil, err
	}

	// release the lock on closing the method:
	defer context.pLock.Unlock()

	// write data on the destination file:
	pDataOffset, err := app.writeDataAndReferenceOnDestinationFile(context, updatedReference)
	if err != nil {
		return nil, nil, err
	}

	// create the source path:
	sourcePath := filepath.Join(app.dirPath, context.name)

	// create the backup path:
	backupFile := fmt.Sprintf("%s%s%s", context.name, fileNameExtensionDelimiter, app.bckExtension)
	backupPath := filepath.Join(app.dirPath, backupFile)

	// copy the source database to a backup file:
	backupPtr, err := os.Create(backupPath)
	if err != nil {
		return nil, nil, err
	}

	_, err = io.Copy(backupPtr, context.pConn)
	if err != nil {
		return nil, nil, err
	}

	// close the backup file:
	err = backupPtr.Close()
	if err != nil {
		return nil, nil, err
	}

	// close the source connection:
	err = context.pConn.Close()
	if err != nil {
		return nil, nil, err
	}

	// delete the source database:
	err = os.Remove(sourcePath)
	if err != nil {
		return nil, nil, err
	}

	// rename the destination database to source:
	destinationFile := fmt.Sprintf("%s%s%s", context.name, fileNameExtensionDelimiter, app.dstExtension)
	destinationPath := filepath.Join(app.dirPath, destinationFile)
	err = os.Rename(destinationPath, sourcePath)
	if err != nil {
		return nil, nil, err
	}

	// delete the backup file:
	err = os.Remove(backupPath)
	if err != nil {
		return nil, nil, err
	}

	// re-open the source connection:
	pNewConn, err := os.Open(sourcePath)
	if err != nil {
		return nil, nil, err
	}

	return pNewConn, pDataOffset, nil
}

func (app *application) writeDataAndReferenceOnDestinationFile(context *context, updatedReference references.Reference) (*uint, error) {
	// destination path:
	destinationFile := fmt.Sprintf("%s%s%s", context.name, fileNameExtensionDelimiter, app.dstExtension)
	destinationPath := filepath.Join(app.dirPath, destinationFile)

	// create the destination file:
	destination, err := os.Create(destinationPath)
	if err != nil {
		return nil, err
	}

	// close the destination:
	defer destination.Close()

	// convert the updated reference to data:
	refData, err := app.referenceToContent(updatedReference)
	if err != nil {
		return nil, err
	}

	// write the reference data on disk:
	writtenAmount, err := destination.Write(refData)
	if err != nil {
		return nil, err
	}

	if writtenAmount != len(refData) {
		str := fmt.Sprintf("%d bytes were expected to be writte while writing the updated reference bytes, %d actually written", len(refData), writtenAmount)
		return nil, errors.New(str)
	}

	// declare the read and write offsets:
	readOffset := int64(context.dataOffset)
	writeOffset := int64(writtenAmount)
	if context.reference != nil {
		if context.reference.HasContentKeys() {
			contentKeys := context.reference.ContentKeys().List()
			for _, oneContentKey := range contentKeys {
				toDelKeyname := app.makeToDeleteKeyname(oneContentKey.Kind(), oneContentKey.Hash())
				if _, ok := context.delList[toDelKeyname]; ok {
					continue
				}

				// fetch the pointer
				pointer := oneContentKey.Content()

				// read the content:
				readOffset = readOffset + int64(pointer.From())
				to := readOffset + int64(pointer.Length())
				chunkSize := to - readOffset
				contentBytes := make([]byte, chunkSize)
				amountRead, err := context.pConn.ReadAt(contentBytes, readOffset)
				if err != nil {
					break
				}

				if chunkSize != int64(amountRead) {
					str := fmt.Sprintf("%d bytes were expected to be read from source database, %d actually read", chunkSize, amountRead)
					return nil, errors.New(str)
				}

				// write content on destination:
				err = app.saveDataOnDisk(writeOffset, contentBytes, destination)
				if err != nil {
					break
				}

				// update the write offset:
				writeOffset += int64(len(contentBytes))
			}
		}

	}

	// write the data on disk:
	for _, oneContent := range context.insertList {
		contentBytes := oneContent.Data()
		err = app.saveDataOnDisk(writeOffset, contentBytes, destination)
		if err != nil {
			break
		}

		// update the offset:
		writeOffset += int64(len(contentBytes))
	}

	dataOffset := uint(writtenAmount)
	return &dataOffset, nil
}

func (app *application) referenceToContent(reference references.Reference) ([]byte, error) {
	contentBytes, err := app.referenceAdapter.ToContent(reference)
	if err != nil {
		return nil, err
	}

	bytesLength := make([]byte, expectedReferenceBytesLength)
	binary.LittleEndian.PutUint64(bytesLength, uint64(len(contentBytes)))

	data := []byte{}
	data = append(data, bytesLength...)
	return append(data, contentBytes...), nil
}

func (app *application) saveDataOnDisk(offset int64, data []byte, pConn *os.File) error {
	// seek the file at the from byte:
	seekOffset, err := pConn.Seek(offset, 0)
	if err != nil {
		return err
	}

	if seekOffset != offset {
		str := fmt.Sprintf("the offset was expected to be %d, %d returned after file seek", offset, seekOffset)
		return errors.New(str)
	}

	// write the data on disk:
	amountWritten, err := pConn.Write(data)
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
