"""Typed exceptions raised by pipeline infrastructure."""


class PipelineError(RuntimeError):
    """Base class for pipeline command failures."""


class ConfigError(PipelineError):
    """Raised when project configuration or required credentials are invalid."""


class MissingCredentialError(ConfigError):
    """Raised when a stage-specific credential is not configured."""


class DatabaseError(PipelineError):
    """Raised when database setup, migration, or access fails."""


class MediaCloudError(PipelineError):
    """Raised when Media Cloud infrastructure cannot complete a request."""


class FetchError(PipelineError):
    """Raised when article-fetch infrastructure cannot continue."""


class ExtractionError(PipelineError):
    """Raised when extraction infrastructure cannot continue."""


class ExportError(PipelineError):
    """Raised when export infrastructure cannot complete."""
