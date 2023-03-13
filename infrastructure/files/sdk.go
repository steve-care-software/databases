package files

import (
	databases "github.com/steve-care-software/databases/applications"
	"github.com/steve-care-software/databases/domain/contents"
	"github.com/steve-care-software/databases/domain/references"
	"github.com/steve-care-software/libs/cryptography/trees"
)

const fileNameExtensionDelimiter = "."
const expectedReferenceBytesLength = 8
const filePermission = 0777

// NewApplication creates a new file application instance
func NewApplication(
	dirPath string,
	dstExtension string,
	bckExtension string,
	readChunkSize uint,
) databases.Application {
	contentsBuilder := contents.NewBuilder()
	contentBuilder := contents.NewContentBuilder()
	referenceAdapter := references.NewAdapter()
	referenceBuilder := references.NewBuilder()
	referenceContentKeysBuilder := references.NewContentKeysBuilder()
	referenceContentKeyBuilder := references.NewContentKeyBuilder()
	referenceCommitsBuilder := references.NewCommitsBuilder()
	referenceCommitAdapter := references.NewCommitAdapter()
	referenceCommitBuilder := references.NewCommitBuilder()
	referenceActionBuilder := references.NewActionBuilder()
	referencePointerBuilder := references.NewPointerBuilder()
	hashTreeBuilder := trees.NewBuilder()
	return createApplication(
		contentsBuilder,
		contentBuilder,
		referenceAdapter,
		referenceBuilder,
		referenceContentKeysBuilder,
		referenceContentKeyBuilder,
		referenceCommitsBuilder,
		referenceCommitAdapter,
		referenceCommitBuilder,
		referenceActionBuilder,
		referencePointerBuilder,
		hashTreeBuilder,
		dirPath,
		dstExtension,
		bckExtension,
		readChunkSize,
	)
}
