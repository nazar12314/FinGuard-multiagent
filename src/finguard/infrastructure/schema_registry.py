import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class SchemaRegistryError(Exception):
    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class SchemaRegistry:
    """The Confluent-compatible API of the schema registry (Apicurio serves it too)."""

    def __init__(self, url: str, *, timeout_s: float = 10.0) -> None:
        self._url = url.rstrip("/")
        self._timeout_s = timeout_s

    def register(self, subject: str, schema: str) -> int:
        """Register `schema` under `subject` with backward compatibility enforced.

        Returns the schema id; registering an unchanged schema again returns the same id.
        """
        self._call("PUT", f"/config/{quote(subject, safe='')}", {"compatibility": "BACKWARD"})
        response = self._call(
            "POST",
            f"/subjects/{quote(subject, safe='')}/versions",
            {"schemaType": "JSON", "schema": schema},
        )
        schema_id: int = response["id"]
        return schema_id

    def is_compatible(self, subject: str, schema: str) -> bool | None:
        """Whether `schema` can replace the latest version, or None for a new subject."""
        # The registry answers "compatible" for an unknown subject, so ask about it first.
        try:
            self._call("GET", f"/subjects/{quote(subject, safe='')}/versions")
        except SchemaRegistryError as error:
            if error.status == 404:
                return None
            raise
        response = self._call(
            "POST",
            f"/compatibility/subjects/{quote(subject, safe='')}/versions/latest",
            {"schemaType": "JSON", "schema": schema},
        )
        compatible: bool = response["is_compatible"]
        return compatible

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        request = urllib.request.Request(
            self._url + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": _CONTENT_TYPE, "Accept": _CONTENT_TYPE},
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_s) as response:
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as error:
            raise SchemaRegistryError(
                f"{method} {path} returned {error.code}: {error.read().decode()[:500]}",
                status=error.code,
            ) from None
        except OSError as error:  # refused, unresolvable host or timed out
            reason = getattr(error, "reason", error)
            raise SchemaRegistryError(
                f"{method} {path}: the registry at {self._url} is unreachable ({reason})"
            ) from None
