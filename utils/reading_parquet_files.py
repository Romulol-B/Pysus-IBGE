import pandas as pd
import sys
import os
from dotenv import load_dotenv
from pathlib  import Path
def main():
    load_dotenv()
    #ducklake_location=os.environ.get("DUCKLAKE_LOCATION")
    ducklake_location= os.path.abspath("sinan/")
    ducklake_path=os.path.normpath(os.path.join(ducklake_location,"DENGBR00.parquet"))
    df = pd.read_parquet(ducklake_path)
    return df.head()
if __name__ == "__main__":
    print(main())
