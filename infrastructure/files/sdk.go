package files

import (
	databases "github.com/steve-care-software/databases/applications"
	"github.com/steve-care-software/libs/cryptography/trees"
	"github.com/steve-care-software/databases/domain/contents"
	"github.com/steve-care-software/databases/domain/references"
)

const fileNameExtensionDelimiter = "."
const expectedReferenceBytesLength = 8
const filePermission = 0777

// NewApplication creates a new file application instance
func NewApplication(
	miningValue byte,
	dirPath string,
	dstExtension string,
	bckExtension string,
	readChunkSize uint,
) databases.Application {
	contentsBuilder := contents.NewBuilder()
	contentBuilder := contents.NewContentBuilder()
	referenceAdapter := references.NewAdapter(miningValue)
	referenceBuilder := references.NewBuilder()
	referenceContentKeysBuilder := references.NewContentKeysBuilder()
	referenceContentKeyBuilder := references.NewContentKeyBuilder()
	referenceCommitsBuilder := references.NewCommitsBuilder()
	referenceCommitAdapter := references.NewCommitAdapter(miningValue)
	referenceCommitBuilder := references.NewCommitBuilder(miningValue)
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
		referencePointerBuilder,
		hashTreeBuilder,
		dirPath,
		dstExtension,
		bckExtension,
		readChunkSize,
	)
}
