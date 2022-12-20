class CreateServerFolderError(Exception):
	#Raised when the creating folder on server fails.
	pass

class CleanFolderError(Exception):
	#Raised when the clean folder fails.
	pass

class FolderNotFoundError(Exception):
	pass

class RemoveFileError(Exception):
	pass