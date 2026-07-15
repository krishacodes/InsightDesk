from backend.services.sentiment.roberta_adapter import (
    RoBERTaAdapter
)

model = RoBERTaAdapter()

samples = [

    "The UI is excellent.",

    "The product is okay.",

    "The system crashes frequently.",

    "Billing issues are causing losses.",

    "Customer support was amazing."
]

for sample in samples:

    print("\nTEXT:")
    print(sample)

    print(
        model.predict(
            sample
        )
    )