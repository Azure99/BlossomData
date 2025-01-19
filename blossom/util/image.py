import base64
import io
from typing import Optional

from PIL import Image


def encode_image_to_base64(
    img: Image.Image, target_size: Optional[int] = None, fmt: str = "JPEG"
) -> str:
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    width, height = img.size
    if target_size and (width > target_size or height > target_size):
        img.thumbnail((target_size, target_size))

    with io.BytesIO() as buffer:
        img.save(buffer, format=fmt)
        img_raw = buffer.getvalue()
        return base64.b64encode(img_raw).decode("utf-8")


def encode_image_to_url(
    img: Image.Image, target_size: Optional[int] = None, fmt: str = "JPEG"
) -> str:
    return f"data:image/{fmt.lower()};base64,{encode_image_to_base64(img, target_size, fmt=fmt)}"


def encode_image_file_to_base64(
    file_path: str, target_size: Optional[int] = None, fmt: str = "JPEG"
) -> str:
    with Image.open(file_path) as img:
        return encode_image_to_base64(img, target_size, fmt)


def encode_image_file_to_url(
    file_path: str, target_size: Optional[int] = None, fmt: str = "JPEG"
) -> str:
    return f"data:image/{fmt.lower()};base64,{encode_image_file_to_base64(file_path, target_size, fmt=fmt)}"
