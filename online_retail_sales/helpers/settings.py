from online_retail_sales.helpers.crypto.cipher import DataCipher
from dotenv import load_dotenv
import os


load_dotenv()

environment = os.getenv('ENVIRONMENT', None)
print("ENV: ", environment)

if environment is None:
    raise ValueError('ENVIRONMENT is not set.')


def get_connection_string():
    secret_key = os.getenv('SECRET_KEY')
    assert secret_key is not None, "SECRET_KEY environment variable must be set."

    decipher = DataCipher(hex_key=secret_key)

    db_host = os.getenv('POSTGRES_HOST')
    db_port = os.getenv('POSTGRES_PORT')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB_NAME')

    assert all([db_host, db_port, db_user, db_password, db_name]), "All connection variables must be set."

    decrypted_vars = {
        'POSTGRES_HOST': decipher.decrypt_data(db_host),
        'POSTGRES_PORT': decipher.decrypt_data(db_port),
        'POSTGRES_USER': decipher.decrypt_data(db_user),
        'POSTGRES_PASSWORD': decipher.decrypt_data(db_password),
        'POSTGRES_DB_NAME': decipher.decrypt_data(db_name)
    }

    connection_string = f"postgresql+psycopg2://{decrypted_vars['POSTGRES_USER']}:{decrypted_vars['POSTGRES_PASSWORD']}@" \
                        f"{decrypted_vars['POSTGRES_HOST']}:{decrypted_vars['POSTGRES_PORT']}/" \
                        f"{decrypted_vars['POSTGRES_DB_NAME']}"

    return connection_string
