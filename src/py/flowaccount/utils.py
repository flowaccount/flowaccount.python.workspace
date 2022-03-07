import re


def format_snake_case(camel_case: str) -> str:
    pattern = re.compile(r"[A-Z]")
    lower_words = pattern.split(camel_case)
    big_chars = pattern.findall(camel_case)
    words = [t[0] + t[1] for t in zip([""] + big_chars, lower_words)]
    return ("_").join(words).lower()
