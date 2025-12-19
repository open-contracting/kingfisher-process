class KingfisherProcessError(Exception):
    """Base class for exceptions from within this project."""


class AlreadyExists(KingfisherProcessError):  # noqa: N818
    """Raised if an object is already saved to the database."""


class InvalidFormError(KingfisherProcessError, ValueError):
    """Raised if a form is invalid."""


class EmptyFormatError(KingfisherProcessError):
    """Raised if a collection file contains no data."""


class UnsupportedFormatError(KingfisherProcessError):
    """Raised if a collection file's format is unsupported."""
