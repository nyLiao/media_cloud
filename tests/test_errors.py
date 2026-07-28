from mc_pipeline.errors import (
    ConfigError,
    DatabaseError,
    ExportError,
    ExtractionError,
    FetchError,
    MediaCloudError,
    MissingCredentialError,
    PipelineError,
)


def test_pipeline_errors_share_one_root():
    error_types = (
        ConfigError,
        MissingCredentialError,
        DatabaseError,
        MediaCloudError,
        FetchError,
        ExtractionError,
        ExportError,
    )

    assert all(issubclass(error_type, PipelineError) for error_type in error_types)
