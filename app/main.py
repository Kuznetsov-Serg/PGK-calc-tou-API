import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from app.api.router import router, router_test
from app.auth.crud import check_token
from app.auth.router import auth_router
from app.core import models
from app.core.crud import fact_fully_loaded_slow_background

from app.core.database import EnginePostresql
from app.settings import Configuration, load_configuration
from app.utils.exceptions import api_error_responses, http_exception_handler, validation_exception_handler
from app.utils.responses import WrappedResponse
from app.utils.sentry import init_sentry


class CheckApiKey(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # print('configuration=', configuration)
        print("request=", request)
        if configuration.AUTHORISE_BY_TOKEN:
            token = request.headers.get("authorization", None)
            print(f"CheckApiKey token={token}")
            try:
                token = check_token(token[7:])
            except:
                print("Wrong token")
                return JSONResponse(
                    status_code=401,
                    content={"error_code": "server_problem", "errorMessage": "Not authenticated"},
                )
                # response = Response(status_code=401, content=b'{"error_code": "server_problem", "errorMessage": "Not authenticated"}')
                # response = await call_next(request)
                return response
            else:
                request.headers["username"] = token["sub"]
        response = await call_next(request)

        return response


def create_app(config: Configuration) -> FastAPI:
    root_path = f"/v1/{config.PROJECT_NAME}"
    if config.PROJECT_ENVIRONMENT == "local":
        root_path = ""

    application = FastAPI(
        title=config.PROJECT_NAME,
        description=f"Environment: {config.PROJECT_ENVIRONMENT}",
        version=config.PROJECT_VERSION,
        default_response_class=WrappedResponse,
        root_path=root_path,
    )

    application.add_middleware(
        CORSMiddleware,
        # allow_origins=[str(origin) for origin in config.BACKEND_CORS_ORIGINS],
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    application.add_middleware(
        GZipMiddleware,
        minimum_size=config.GZIP_MINIMUM_SIZE,
    )
    # application.add_middleware(CheckApiKey)
    if config.SENTRY:
        init_sentry()
        application.add_middleware(SentryAsgiMiddleware)

    application.exception_handler(HTTPException)(http_exception_handler)
    application.exception_handler(RequestValidationError)(validation_exception_handler)
    application.state.config = config
    application.include_router(auth_router, tags=["Authorization"])
    application.include_router(router_test, responses=api_error_responses)
    application.include_router(router, responses=api_error_responses)
    # use_route_names_as_operation_ids(application)
    application.mount(
        "/static", StaticFiles(directory=Path(__file__).parent.parent.absolute() / "static"), name="/static"
    )

    return application


configuration = load_configuration()
# setup_logging(configuration.logging)
app = create_app(configuration)


# @app.middleware("http")
# async def add_process_time_header(request: Request, call_next, token=Depends(check_token)):
# # async def add_process_time_header(request: Request, call_next, token: str = Depends(oauth2_scheme)):
#     start_time = time.time()
#     # token = await check_token_wrap(token)
#     print(f'!!! token={token}')
#     response = await call_next(request)
#     print(f'!!!_!!! token={token}')
#     process_time = time.time() - start_time
#     response.headers["X-Process-Time"] = str(process_time)
#     return response


@app.on_event("startup")
async def startup_event() -> None:
    """tasks to do at server startup"""
    # asyncio.create_task(fact_fully_loaded_slow_background(10), name="Background_task")
    # fastapi.BackgroundTasks().add_task(asyncio.run, fact_fully_loaded_slow_background, 10)
    # create and start the daemon thread
    print("Starting background task... (every day = 86400sec)")
    loop = asyncio.get_running_loop()
    asyncio.gather(
        loop.run_in_executor(None, fact_fully_loaded_slow_background, 86400),
    )
    # daemon = Thread(target=fact_fully_loaded_slow_background, args=(86400,), daemon=True, name="Background")
    # daemon.start()
    # # main thread is carrying on...
    # print(f"Main thread is carrying on...ID={daemon.ident}")


models.Base.metadata.create_all(bind=EnginePostresql)
