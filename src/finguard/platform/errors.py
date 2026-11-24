from typing import ClassVar


class ApplicationError(Exception):
    """Base for errors raised by application services.

    `code` is part of the public API error shape, so it must stay stable. The API layer
    maps each subclass to an HTTP status; services never deal in status codes.
    """

    code: ClassVar[str] = "application_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class NotFound(ApplicationError):
    code = "not_found"

    def __init__(self, resource: str, resource_id: object) -> None:
        super().__init__(f"{resource} {resource_id} not found")
        self.resource = resource
        self.resource_id = str(resource_id)


class Conflict(ApplicationError):
    code = "conflict"


class Forbidden(ApplicationError):
    code = "forbidden"


class ValidationFailed(ApplicationError):
    code = "validation_failed"


class DependencyUnavailable(ApplicationError):
    """A dependency (database, Kafka, storage, LLM provider) is down; safe to retry."""

    code = "dependency_unavailable"
