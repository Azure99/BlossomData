import math
from collections import Counter


class CharRepetitionFilter:
    def __init__(
        self, n: int = 10, min_ratio: float = 0.0, max_ratio: float = 0.5
    ) -> None:
        self.n = n
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio

    def filter(self, text: str) -> bool:
        char_ngrams = [text[i : i + self.n] for i in range(len(text) - self.n + 1)]
        freq_char_ngrams = Counter(char_ngrams)

        if len(freq_char_ngrams) == 0:
            return True

        freq_values = sorted(freq_char_ngrams.values(), reverse=True)
        num_no_rep_char_ngrams = sum(1 for count in freq_values if count == 1)

        num_rep_char_ngrams = min(
            int(math.sqrt(len(freq_values))), len(freq_values) - num_no_rep_char_ngrams
        )

        total_repetitions = sum(freq_values[:num_rep_char_ngrams])
        total_ngrams = sum(freq_values)

        rep_ratio = total_repetitions / total_ngrams if total_ngrams != 0 else 0.0

        return self.min_ratio <= rep_ratio <= self.max_ratio
