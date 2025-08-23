# from pydantic_settings import BaseSettings
#
# class BERDLSettings(BaseSettings):
#     CTS_URL: str
#


import os
import warnings

# --- Check for Configuration Errors (Simpler Version) ---

ERROR_ENV_VAR = "MINIO_CONFIG_ERROR"
error_message = os.environ.get(ERROR_ENV_VAR)

if error_message:
    full_warning = (
        f"Critical Environment Warning: {error_message} "
        "Code relying on Spark/S3 may fail."
    )
    warnings.warn(full_warning, UserWarning)
