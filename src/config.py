import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    def __init__(self):
        #SQLite database path
        self.db_path = self.get_db_path()
        
        #Error log path
        self.error_log_path = self.get_error_log_path()

        #LLM settings
        #TODO Check what actually needs to be done here 
        self.llm_model = self.get_llm_model()
        self.llm_API_key = self.get_llm_API_key()
        
        #Check if all settings are set
        self.check_settings()
        

    def get_db_path(self) -> str:
        os.getenv("DB_PATH") if os.getenv("DB_PATH") else "data/database.db"

    def get_error_log_path(self) -> str:
        os.getenv("ERROR_LOG_Path") if os.getenv("CSV_PATH") else "error.log"

    def get_llm_model(self) -> str:
        os.getenv("LLM_MODEL") if os.getenv("LLM_MODEL") else "" #TODO Add default model

    def get_llm_API_key(self) -> str:
        os.getenv("LLM_API_KEY") if os.getenv("LLM_API_KEY") else "" #TODO Add default API key

    def check_settings(self) -> None:
        #Check if all settings are set and raise an error if not
        if self.llm_API_key == "":
            raise ValueError("LLM API key is not set")
        if self.llm_model == "":
            raise ValueError("LLM model is not set")
        if self.db_path == "":
            raise ValueError("SQLite database path is not set")
        if self.error_log_path == "":
            raise ValueError("Error log path is not set")