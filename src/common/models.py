from pydantic import BaseModel


class StorageLocation(BaseModel):
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"
