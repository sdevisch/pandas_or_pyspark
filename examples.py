from unipandas import configure_backend, read_csv


def main():
    # Choose a backend at runtime. Comment/uncomment to try others.
    # configure_backend("pandas")
    # configure_backend("dask")
    # configure_backend("pyspark")

    # Replace with an actual CSV path for testing
    path = "./data/example.csv"
    df = read_csv(path)
    print("Backend:", df.backend)

    out = (
        df.select(["a", "b"])  # type: ignore
          .assign(c=lambda x: x["a"] + x["b"])  # type: ignore
          .query("a > 0")
    )

    agg = out.groupby("a").agg({"c": "sum"})
    print(agg.head().to_pandas())


if __name__ == "__main__":
    main()


