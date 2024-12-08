from typing import Any


class Logger:
    def __init__(self) -> None:
        import logging

        self.logger = logging.getLogger(__package__)
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
        )

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self.logger.critical(msg, *args, **kwargs)

    def exception(
        self, msg: Any, *args: Any, exc_info: bool = True, **kwargs: Any
    ) -> None:
        self.logger.error(msg, *args, exc_info=exc_info, **kwargs)

    def enable(self) -> None:
        self.logger.disabled = False

    def disable(self) -> None:
        self.logger.disabled = True

    def set_level(self, level: int) -> None:
        self.logger.setLevel(level)


logger = Logger()
