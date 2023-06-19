import os
import time
import argparse
import pandas as pd
import numpy as np
from faker import Faker
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

username = os.getenv("POSTGRES_DATABASE_USER")
password = os.getenv("POSTGRES_DATABASE_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DATABASE")
table = os.getenv("POSTGRES_TABLE")


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


faker = Faker()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate fake data...")
    parser.add_argument(
        "--interval",
        type=int,
        default=0.005,
        help="interval of generating fake data in seconds",
    )
    parser.add_argument("-n", type=int, default=1, help="sample size")
    parser.add_argument(
        "--connection-string",
        "-cs",
        dest="connection_string",
        type=str,
        default=f"postgresql://{username}:{password}@{host}:{port}/{database}",
        help="Connection string to the database",
    )
    parser.add_argument(
        "--silent",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="print fake data",
    )

    args = parser.parse_args()

    print(f"Args parsed:")
    print(f"Interval: {args.interval}")
    print(f"Sample size: {args.n}")

    engine = create_engine(args.connection_string)

    print("Iniciando a simulacao...", end="\n\n")

    while True:
        username = [faker.user_name() for i in range(args.n)]
        tweet = [faker.text() for i in range(args.n)]
        date = [faker.date_this_year() for i in range(args.n)]

        df = pd.DataFrame(
            {
                "username": username,
                "tweet": tweet,
                "date": date,
            }
        )

        df.to_sql("tweets", con=engine, if_exists="append", index=False)

        if not args.silent:
            print(df, end="\n\n")

        time.sleep(args.interval)
