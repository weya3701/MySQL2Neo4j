import datetime
import importlib
from neo4jmodels.base.fields import Field
from neo4jmodels.base.models import Model


class ImportException(Model):

    file = Field("file")
    offset = Field("offset")
    limit = Field("limit")
    error = Field("error", callback_func=lambda x: str(x))
    log_time = Field(
        "log_time",
        default_value=datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


class ImportSuccessResult(Model):
    file = Field("file")
    status = Field("status", default_value="Successfult")
    log_time = Field(
        "log_time",
        default_value=datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


class ImportFailResult(Model):
    file = Field("file")
    offset = Field("offset")
    limit = Field("limit")
    status = Field("status", default_value="Import Failed")
    log_time = Field(
        "log_time",
        default_value=datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


class WriteImportLog(object):

    def __init__(self, logger=None, level="info"):
        self.formatter = None
        self.logger = logger
        self.level = level

    def _restore_formatter(self):
        self.formatter = None

    def set_formatter(self, formatter):
        module = importlib.import_module("models.logFormatter.formatter")
        if hasattr(module, formatter):
            self.formatter = getattr(module, formatter)
        return self

    def write_log(self, **data):
        if self.formatter:
            msg = self.formatter(**data).convert()
        else:
            msg = data
        if hasattr(self.logger, self.level):
            getattr(self.logger, self.level)(msg)
        self._restore_formatter()
