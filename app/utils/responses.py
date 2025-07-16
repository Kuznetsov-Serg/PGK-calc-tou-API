import orjson
import typing
from starlette.responses import JSONResponse


class WrappedResponse(JSONResponse):
    def render(self, content: typing.Any) -> bytes:
        return orjson.dumps(content)
