from __future__ import annotations

from PIL import Image

from blossom.util.image import (
    encode_image_file_to_base64,
    encode_image_file_to_data_url,
    encode_image_to_base64,
    encode_image_to_data_url,
)


def test_encode_image_to_base64_and_data_url() -> None:
    img = Image.new("RGB", (2, 2), color="red")
    b64 = encode_image_to_base64(img, fmt="JPEG")
    assert isinstance(b64, str)
    assert len(b64) > 0

    data_url = encode_image_to_data_url(img, fmt="JPEG")
    assert data_url.startswith("data:image/jpeg;base64,")


def test_encode_image_file_to_base64_and_data_url(tmp_path) -> None:
    img = Image.new("RGB", (2, 2), color="blue")
    path = tmp_path / "sample.jpg"
    img.save(path, format="JPEG")

    b64 = encode_image_file_to_base64(str(path))
    assert isinstance(b64, str)
    assert len(b64) > 0

    data_url = encode_image_file_to_data_url(str(path))
    assert data_url.startswith("data:image/jpeg;base64,")
