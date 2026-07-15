from backend.services.sentiment.sentiment_service import (
    analyze_sentiment
)

samples = [

    "The support team was amazing.",

    "The product is okay and works as expected.",

    "The application crashes every day.",

    "Billing issues caused significant losses.",

    "The dashboard UI is clean and intuitive."
]

print("\n========== ROBERTA TESTS ==========\n")

for sample in samples:

    print(f"TEXT: {sample}")

    result = analyze_sentiment(
        text=sample,
        model_name="roberta"
    )

    print(result)
    print("-" * 50)


print("\n========== DISTILBERT TESTS ==========\n")

for sample in samples:

    print(f"TEXT: {sample}")

    result = analyze_sentiment(
        text=sample,
        model_name="distilbert"
    )

    print(result)
    print("-" * 50)


