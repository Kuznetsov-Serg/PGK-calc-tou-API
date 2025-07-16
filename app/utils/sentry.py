import sentry_sdk

from app.settings import PARSED_CONFIG


def init_sentry():
    if PARSED_CONFIG.SENTRY:
        sample_rate = 1.0 if PARSED_CONFIG.PROJECT_ENVIRONMENT != "prod" else 0.2
        sentry_sdk.init(
            PARSED_CONFIG.SENTRY_DSN,
            traces_sample_rate=sample_rate,
            environment=PARSED_CONFIG.PROJECT_ENVIRONMENT,
            release=PARSED_CONFIG.PROJECT_VERSION,
        )
