from pydantic import BaseModel


class CreatePostBody(BaseModel):
    content: str