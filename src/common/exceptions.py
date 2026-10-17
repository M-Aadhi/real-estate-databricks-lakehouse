"""Pipeline exceptions. Each one maps to a failure mode we want to report clearly, never swallow."""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """Configuration file is missing, invalid or incomplete."""


class MissingSourceFileError(PipelineError):
    """An expected source file does not exist."""


class EmptySourceFileError(PipelineError):
    """A source file exists but has no data rows."""


class SchemaValidationError(PipelineError):
    """A source file is missing required columns."""


class DataQualityThresholdError(PipelineError):
    """Too many rows failed validation; the batch is rejected for review."""


class TableWriteError(PipelineError):
    """Writing or merging a Delta table failed."""
