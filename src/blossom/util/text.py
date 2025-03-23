import re


def calculate_edit_distance(s1: str, s2: str) -> int:
    n, m = len(s1), len(s2)
    if n < m:
        s1, s2 = s2, s1
        n, m = m, n

    prev = list(range(m + 1))
    curr = [0] * (m + 1)

    for i in range(1, n + 1):
        curr[0] = i
        for j in range(1, m + 1):
            if s1[i - 1] == s2[j - 1]:
                curr[j] = prev[j - 1]
            else:
                curr[j] = min(prev[j - 1] + 1, curr[j - 1] + 1, prev[j] + 1)
        prev, curr = curr, prev

    return prev[m]


def replace_text(
    text: str, replacements: dict[str, str], case_sensitive: bool = True
) -> str:
    for old, new in replacements.items():
        if not case_sensitive:
            text = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
        else:
            text = text.replace(old, new)
    return text
