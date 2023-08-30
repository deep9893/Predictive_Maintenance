import os,sys 
import pandas as pd
import numpy as np
from src.logger import logging
from src.utils import *
from src.exception import CustomException
from src.entity.config_entity import *
from src.entity.artifact_entity import *
from src.config.configuration import *
import tarfile
from six.moves import urllib
from sklearn.model_selection import StratifiedShuffleSplit


class DataIngestion:
    
    def __init__(self,data_ingestion_config:DataIngestionconfig ):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise CustomException(e,sys)
        
        # download the data
        # extract the data
        # split the data into train and test
        
    def download_maintanance_data(self)->str:
        try:
            download_url = self.data_ingestion_config.dataset_download_url
            
            # folder location to download the data
            tgz_download_dir = self.data_ingestion_config.tgz_download_dir
            
            if os.path.exists(tgz_download_dir):
                os.remove(tgz_download_dir)
                
            os.makedirs(tgz_download_dir,exist_ok = True)
            
            maintanance_file_name = os.path.basename(download_url)
            
            tgz_file_path = os.path.join(tgz_download_dir,maintanance_file_name)
            
            logging.info(f"Downloading file from : [{download_url}] into : [{tgz_file_path}]")
            
            urllib.request.urlretrieve(download_url,tgz_file_path)
        
            logging.info(f"file : [{tgz_file_path}] has been downloaded successfully")
            
            return tgz_file_path
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
        
    def extract_tgz_file(self,tgz_file_path:str):
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            
            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)
                
            os.makedirs(raw_data_dir, exist_ok=True)
            
            logging.info(f"extracting file : [{tgz_file_path}] into dir:[{raw_data_dir}]")
            with tarfile.open(tgz_file_path) as maintanance_tgz_file_obj:
                maintanance_tgz_file_obj.extractall(path=raw_data_dir)
            logging.info(f"extracting completed")
            
        except Exception as e:
            raise CustomException(e,sys) from e
        
        
    def split_data(self)->DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            
            file_name = os.listdir(raw_data_dir)[0]
            
            maintanance_file_path = os.path.join(raw_data_dir,file_name)
            
            logging.info(f"reading csv file :[{maintanance_file_path}]" )
            maintanance_data_frame = pd.read_csv(maintanance_file_path)
            
            maintanance_data_frame['Current_Life_time'] = pd.cut(maintanance_data_frame['Life_Ratio'],
                                                                 bins=[0.0,0.6,0.8,np.inf],
                                                                 labels=[0,1,2])
            
            logging.info(f"splitting data into train & test")
            
            strat_train_set =None
            strat_test_set =None
            
            split = StratifiedShuffleSplit(n_splits=1, test_size=0.2,random_state=42)
            
            for train_index, test_index in split.split(maintanance_data_frame,maintanance_data_frame['Current_Life_time']):
                strat_train_set = maintanance_data_frame.loc[train_index].drop(['Current_Life_time'],axis=1)
                strat_test_set = maintanance_data_frame.loc[test_index].drop(['Current_Life_time'],axis=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                           file_name)
            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,file_name)
            
            
            if strat_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir, exist_ok=True)
                logging.info(f"exporting training dataset to file:[{train_file_path}]")
                strat_train_set.to_csv(train_file_path,index=False)
                
            if strat_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok=True)
                logging.info(f"exporting training dataset to file:[{test_file_path}]")
                strat_test_set.to_csv(test_file_path,index=False)
        
            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                                            test_file_path=test_file_path,
                                                            is_ingested=True,
                                                            message=f"data ingestion completed successfully")
            
            logging.info(f"data ingestion artifact: [{data_ingestion_artifact}]")
            
            return data_ingestion_artifact
        
        except Exception as e:
            raise CustomException(e,sys) from e
      
    
    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            tgz_file_path = self.download_maintanance_data()
            self.extract_tgz_file(tgz_file_path=tgz_file_path)
            return self.split_data()
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
        
        
    def __del__(self):
        logging.info(f"{'>>>'*20} data ingestion log completed.{'>>>'*20} \n\n")