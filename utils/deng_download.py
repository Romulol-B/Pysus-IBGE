from pysus import sinan
def main():
    """download do dataset em parquet dos ultimos 23 anos
        Esses arquivos foram para a pasta (HOME/pysus/ducklake/sinan)
    """
    selected_years = [x +2000 for x in range(23)]
    df = sinan(disease="DENG",year=selected_years)
    return df
if __name__ == "__main__":
    print(main())
