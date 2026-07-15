from backend.services.sentiment.base_model import (
    BaseSentimentModel
)

try:

    model = BaseSentimentModel()

except TypeError as e:

    print("PASS")
    print(e)