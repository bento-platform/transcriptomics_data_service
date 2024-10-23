__all__ = [
    "AuthzException",
]


class AuthzException(Exception):
    def __init__(self, message="Unauthorized", status_code=401) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
