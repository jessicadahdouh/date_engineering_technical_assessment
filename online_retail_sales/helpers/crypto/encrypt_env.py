from cipher import DataCipher

# generate the key using the generate_secret_key file
cipher = DataCipher(hex_key="8d0bcf416b83a24af7b7afdf60f963b51341af0f65b1fe3d0ddfb4ad5ab80ede")
cipher_list = {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_USER": "postgres",
                "POSTGRES_PASSWORD": "12",
                "POSTGRES_DB_NAME": "retail_sales"
                }

for key, value in cipher_list.items():
    encrypted_value = cipher.encrypt_data(value)
    print(f"{key}: {encrypted_value}")
