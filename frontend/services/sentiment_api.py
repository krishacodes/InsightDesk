# services/sentiment_api.py


def get_sentiment_data():

    """
    Temporary mock data.

    Future:
        FastAPI
            ↓
        GET /dashboard/sentiment
            ↓
        response.json()

    """

    return {

        # ----------------------------------
        # KPI CARDS
        # ----------------------------------

        "positive": "24%",

        "negative": "71%",

        "neutral": "5%",

        "confidence": "88%",


        # ----------------------------------
        # SENTIMENT TREND
        # ----------------------------------

        "trend": [

            {
                "day": "Mon",
                "positive": 40,
                "neutral": 90,
                "negative": 50
            },

            {
                "day": "Tue",
                "positive": 45,
                "neutral": 95,
                "negative": 70
            },

            {
                "day": "Wed",
                "positive": 38,
                "neutral": 88,
                "negative": 69
            },

            {
                "day": "Thu",
                "positive": 50,
                "neutral": 100,
                "negative": 80
            },

            {
                "day": "Fri",
                "positive": 55,
                "neutral": 110,
                "negative": 90
            },

            {
                "day": "Sat",
                "positive": 30,
                "neutral": 60,
                "negative": 30
            },

            {
                "day": "Sun",
                "positive": 25,
                "neutral": 65,
                "negative": 210
            }

        ],


        # ----------------------------------
        # EMOTION DISTRIBUTION
        # ----------------------------------

        "emotions": {

            "anger": 43,

            "sadness": 18,

            "fear": 11,

            "joy": 15,

            "neutral": 9,

            "surprise": 4

        },


        # ----------------------------------
        # MODEL COMPARISON
        # ----------------------------------

        "models": {

            "RoBERTa": {

                "Macro F1": 0.83,

                "Precision": 0.84,

                "Recall": 0.81,

                "Accuracy": 0.86,

                "Latency": "310 ms",

                "Throughput": "3.2/sec"

            },

            "DistilBERT": {

                "Macro F1": 0.78,

                "Precision": 0.80,

                "Recall": 0.76,

                "Accuracy": 0.82,

                "Latency": "95 ms",

                "Throughput": "9.8/sec"

            }

        },


        # ----------------------------------
        # TOP NEGATIVE CASES
        # ----------------------------------

        "negative_cases": [

            {

                "case_id": 71,

                "severity": "HIGH",

                "department": "Engineering",

                "emotion": "ANGER",

                "confidence": "97%",

                "timestamp": "Today · 09:14",

                "representative_text":
                "App crashes every time I open it after the latest update."

            },


            {

                "case_id": 82,

                "severity": "HIGH",

                "department": "Authentication",

                "emotion": "ANGER",

                "confidence": "94%",

                "timestamp": "Today · 08:52",

                "representative_text":
                "Unable to login despite resetting my password multiple times."

            },


            {

                "case_id": 91,

                "severity": "MEDIUM",

                "department": "Customer Success",

                "emotion": "SADNESS",

                "confidence": "91%",

                "timestamp": "Yesterday · 22:30",

                "representative_text":
                "Ticket has been open for twelve days without any response."

            }

        ]

    }