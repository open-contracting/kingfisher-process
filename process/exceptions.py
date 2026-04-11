class KingfisherProcessError(Exception):
    """Base class for exceptions from within this project."""


class AlreadyExists(KingfisherProcessError):  # noqa: N818
    """Raised if an object is already saved to the database."""


class InvalidFormError(KingfisherProcessError, ValueError):
    """Raised if a form is invalid."""

    def __init__(self, form):
        self.errors = form.errors.as_data()
        super().__init__(
            "\n".join(
                str(form.error_message_formatter(field, error))
                for field, error_list in self.errors.items()
                for error in error_list
            )
        )


class EmptyFormatError(KingfisherProcessError):
    """Raised if a collection file contains no data."""


class UnsupportedFormatError(KingfisherProcessError):
    """Raised if a collection file's format is unsupported."""
