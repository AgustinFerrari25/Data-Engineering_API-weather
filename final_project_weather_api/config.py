from dotenv import load_dotenv
import os

load_dotenv()

AWS_HOST = os.getenv("AWS_HOST")
AWS_DB = os.getenv("AWS_DB")
AWS_USER = os.getenv("AWS_USER")
AWS_PORT = os.getenv("AWS_PORT")
AWS_PASSWORD = os.getenv("AWS_PASSWORD")
KEY = os.getenv("KEY")