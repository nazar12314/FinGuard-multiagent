from finguard.platform.errors import (
    ApplicationError,
    Conflict,
    DependencyUnavailable,
    Forbidden,
    NotFound,
    ValidationFailed,
)


def test_not_found_names_the_missing_resource() -> None:
    error = NotFound("Case", 42)

    assert error.message == "Case 42 not found"
    assert (error.resource, error.resource_id) == ("Case", "42")


def test_error_codes_are_stable_and_distinct() -> None:
    # Clients branch on these codes, so renaming one is a breaking API change.
    codes = {
        error.code
        for error in (NotFound, Conflict, Forbidden, ValidationFailed, DependencyUnavailable)
    }

    assert codes == {
        "not_found",
        "conflict",
        "forbidden",
        "validation_failed",
        "dependency_unavailable",
    }
    assert all(
        issubclass(error, ApplicationError)
        for error in (NotFound, Conflict, Forbidden, ValidationFailed, DependencyUnavailable)
    )
