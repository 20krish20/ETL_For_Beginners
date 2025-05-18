from etl_code import ETL
from log import Logger

def main():
    logger = Logger()
    etl = ETL(data_dir="data")

    logger.log("ETL Job Started")

    logger.log("Extract phase Started")
    extracted_data = etl.extract()
    logger.log("Extract phase Completed")

    logger.log("Transform phase Started")
    transformed_data = etl.transform(extracted_data)
    print("Transformed data:")
    print(transformed_data)
    logger.log("Transform phase Completed")

    logger.log("Load phase Started")
    etl.load(transformed_data)
    logger.log("Load phase Completed")

    logger.log("ETL Job Completed")

if __name__ == "__main__":
    main()
