import re


def replace_text(
    text: str, replacements: dict[str, str], case_sensitive: bool = True
) -> str:
    for old, new in replacements.items():
        if not case_sensitive:
            text = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
        else:
            text = text.replace(old, new)
    return text
