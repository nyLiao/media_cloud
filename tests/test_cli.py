from __future__ import annotations

from mc_pipeline.cli import _echo_extract_summary
from mc_pipeline.stage import StageSummary


def test_extract_summary_omits_already_reported_request_details(capsys):
    summary = StageSummary(
        "revolving_door_ca",
        processed=2,
        succeeded=2,
        skipped=0,
        failed=0,
        request_details=(
            '{"event":"llm_batch_completed","phase":"screening"}',
            '{"event":"llm_run_completed"}',
        ),
    )

    _echo_extract_summary(summary)

    assert capsys.readouterr().out == (
        "topic=revolving_door_ca processed=2 succeeded=2 skipped=0 failed=0\n"
    )
