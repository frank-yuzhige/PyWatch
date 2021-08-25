
class PyWatchError(Exception):
    def __init__(self, error_msg: str) -> None:
        super().__init__()
        self._error_msg = error_msg

    def __str__(self):
        return (
            f"An error occurred in PyWatch:\n"
            f"{self._error_msg}\n"
        )

