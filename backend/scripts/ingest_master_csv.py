import pandas as pd
from tqdm import tqdm

from backend.models.complaint import ComplaintCreate
from backend.services.duplicate_service import process_complaint

CSV_PATH = "data/master_complaints.csv"


def clean(value):
    """
    Convert Pandas NaN values to None.
    Strip whitespace from strings.
    """

    if pd.isna(value):
        return None

    if isinstance(value, str):

        value = value.strip()

        if value == "":
            return None

    return value


def ingest_master_csv():

    # Load CSV
    df = pd.read_csv(CSV_PATH)
    df = df.iloc[101:]
   

    print(f"\nLoaded {len(df)} complaints\n")

    success = 0
    failed = 0

    for _, row in tqdm(df.iterrows(), total=len(df)):

        try:

            complaint = ComplaintCreate(

                complaint_text=clean(
                    row["complaint_text"]
                ),

                user_id=str(
                    clean(row["user_id"])
                ),

                rating=clean(
                    row["rating"]
                ),

                source=clean(
                    row["source"]
                ),

                product=clean(
                    row["product"]
                ),

                company_size=clean(
                    row["company_size"]
                ),

                is_synthetic=bool(
                    clean(row["is_synthetic"]) or False
                )
            )
            import time

            time.sleep(0.2)
            result = process_complaint(
                complaint
            )

            print("\nResult:")
            print(result)

            success += 1
        except Exception as e:

            import traceback

            failed += 1

            print("\n========================")
            print("FAILED COMPLAINT")
            print("========================")
            print(row["complaint_text"])

            print("\nERROR:")
            print(e)

            print("\n========================")
            print("FULL TRACEBACK")
            print("========================")

            traceback.print_exc()

            break

            print("\n========================")
            print("INGESTION SUMMARY")
            print("========================")

            print(
                f"Successful : {success}"
            )

            print(
                f"Failed     : {failed}"
            )


if __name__ == "__main__":
    ingest_master_csv()