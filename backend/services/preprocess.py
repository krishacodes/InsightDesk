import re

# ==================================================================
# PRODUCT / COMPANY NAME SCRUBBING
# ==================================================================
# Any real product or company name that could appear in scraped
# review text (Capterra / Play Store) must be removed BEFORE the
# text reaches embedding, clustering (BERTopic), or the RCA prompt.
# Otherwise BERTopic tends to cluster by *brand mention* instead of
# *issue type*, and the duplicate-detection threshold ends up
# calibrated on one product's writing style instead of the domain.
#
# This list is data-driven: it should be regenerated from the
# distinct values in the `product` column whenever a new real-data
# source is added, not hand-maintained ad hoc like the old
# {"vision", "helpdesk"} single-product patch was.
#
# Multi-word names are matched as whole phrases FIRST (longest
# first, so "vision helpdesk" is caught before a lone "vision"
# check could), then removed. This is safer than relying on the
# single-word STOPWORDS pass alone, which would silently nuke
# generic words like "help" out of every complaint if a product
# name such as "Help Scout" were added to STOPWORDS word-by-word.

PRODUCT_NAMES = [
    "vision helpdesk",
    "freshdesk",
    "freshworks",
    "zendesk",
    "zoho desk",
    "zoho",
    "happyfox",
    "liveagent",
    "help scout",
    "helpscout",
    "kayako",
    "jira service management",
    "jira service desk",
]

# Sort longest-first so multi-word names are matched before any
# substring/single-word overlap could partially consume them.
_PRODUCT_NAMES_SORTED = sorted(
    PRODUCT_NAMES, key=len, reverse=True
)

_PRODUCT_NAME_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(name) for name in _PRODUCT_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)


def strip_product_names(text: str) -> str:
    """
    Remove known product/company names from complaint text.
    Must run BEFORE lowercasing/tokenizing in clean_text, since it
    matches multi-word phrases.
    """
    return _PRODUCT_NAME_PATTERN.sub(" ", text)


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

}


def clean_text(text: str):

    text = strip_product_names(text)

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