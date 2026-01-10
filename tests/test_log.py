from __future__ import annotations

import logging
from unittest.mock import Mock

from blossom import log as log_module


def test_logger_method_delegation(monkeypatch) -> None:
    mock_logger = Mock()
    monkeypatch.setattr(log_module.logger, "logger", mock_logger)

    log_module.logger.debug("debug")
    mock_logger.debug.assert_called_once_with("debug")

    mock_logger.reset_mock()
    log_module.logger.info("info")
    mock_logger.info.assert_called_once_with("info")

    mock_logger.reset_mock()
    log_module.logger.warning("warning")
    mock_logger.warning.assert_called_once_with("warning")

    mock_logger.reset_mock()
    log_module.logger.error("error")
    mock_logger.error.assert_called_once_with("error")

    mock_logger.reset_mock()
    log_module.logger.critical("critical")
    mock_logger.critical.assert_called_once_with("critical")

    mock_logger.reset_mock()
    log_module.logger.exception("boom")
    mock_logger.error.assert_called_once_with("boom", exc_info=True)


def test_logger_controls(monkeypatch) -> None:
    mock_logger = Mock()
    monkeypatch.setattr(log_module.logger, "logger", mock_logger)

    log_module.logger.disable()
    assert mock_logger.disabled is True

    log_module.logger.enable()
    assert mock_logger.disabled is False

    log_module.logger.set_level(logging.DEBUG)
    mock_logger.setLevel.assert_called_once_with(logging.DEBUG)
