class LabUploadError(Exception):
    """Base exception for laboratory upload processing."""


class InvalidLabUpload(LabUploadError):
    """The uploaded file is empty, malformed, or unsupported."""


class LabImportError(LabUploadError):
    """The file was valid but could not be imported."""