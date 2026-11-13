"""The small OLED on the datalogger (core/display.py on the deployed build).

Technicians read the device code and status off it. On a machine without the
display library or the I²C bus the messages go to the log instead, so the rest of
the agent never has to care.
"""
from __future__ import annotations

import logging

log = logging.getLogger("display")


class Display:
    def __init__(self):
        self.dev = None
        try:  # luma.oled is only installed on the image
            from luma.core.interface.serial import i2c  # type: ignore
            from luma.oled.device import ssd1306  # type: ignore

            self.dev = ssd1306(i2c(port=1, address=0x3C))
        except Exception:  # noqa: BLE001
            self.dev = None

    def show(self, line1: str, line2: str = "") -> None:
        if self.dev is None:
            log.info("[display] %s | %s", line1, line2)
            return
        try:
            from luma.core.render import canvas  # type: ignore

            with canvas(self.dev) as draw:
                draw.text((2, 4), line1[:20], fill="white")
                draw.text((2, 30), line2[:20], fill="white")
        except Exception:  # noqa: BLE001
            log.info("[display] %s | %s", line1, line2)

    def clear(self) -> None:
        if self.dev is not None:
            try:
                self.dev.clear()
            except Exception:  # noqa: BLE001
                pass
