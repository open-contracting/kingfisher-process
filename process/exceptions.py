class KingfisherProcessError(Exception):
    """Base class for exceptions from within this project"""


class AlreadyExists(KingfisherProcessError):
    """Raised if an object is already saved to the database"""


class InvalidFormError(KingfisherProcessError, ValueError):
    """Raised if a form is invalid"""
