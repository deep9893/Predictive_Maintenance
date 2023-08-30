from src.pipeline.pipeline import Pipeline
from src.exception import CustomException
from src.logger import logging
from src.config.configuration import Configuartion
from src.components.data_ingestion import DataIngestion
import os


def main():
    try:
        config_path = os.path.join("config","config.yaml")
        pipeline = Pipeline(Configuartion(config_file_path=config_path))
        #pipeline.run_pipeline()
        pipeline.run_pipeline()
        logging.info("main function execution completed.")


    except Exception as e:
        logging.error(f"{e}")
        print(e)



if __name__=="__main__":
    main()
