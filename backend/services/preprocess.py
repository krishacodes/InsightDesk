import re


import re

STOPWORDS = {

    # articles
    "a",
    "an",
    "the",

    # pronouns
    "i",
    "me",
    "my",
    "mine",
    "you",
    "your",
    "yours",
    "we",
    "our",
    "ours",
    "they",
    "their",
    "theirs",
    "he",
    "his",
    "she",
    "her",
    "hers",
    "it",
    "its",

    # verbs
    "is",
    "am",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "has",
    "have",
    "had",
    "having",
    "do",
    "does",
    "did",

    # auxiliary verbs
    "can",
    "could",
    "will",
    "would",
    "shall",
    "should",
    "may",
    "might",
    "must",

    # connectors
    "and",
    "or",
    "but",
    "if",
    "because",
    "while",
    "although",

    # prepositions
    "to",
    "of",
    "for",
    "from",
    "in",
    "on",
    "at",
    "with",
    "by",
    "into",
    "through",
    "over",
    "under",

    # common fillers
    "this",
    "that",
    "these",
    "those",
    "there",
    "here",
    "very",
    "much",
    "some",
    "any",
    "all",
    "few",
    "many",

    # complaint fillers
    "please",
    "kindly",
    "thanks",
    "thank",
    "hello",
    "hi",

    # time fillers
    "today",
    "yesterday",
    "tomorrow",

    # domain specific
    "vision",
    "helpdesk"

}


def clean_text(text: str):

    text = text.lower()

    text = re.sub(
        r"[^a-z0-9\s]",
        " ",
        text
    )

    words = text.split()

    words = [

        word

        for word in words

        if word not in STOPWORDS

    ]

    text = " ".join(words)

    text = re.sub(
        r"\s+",
        " ",
        text
    ).strip()

    return text