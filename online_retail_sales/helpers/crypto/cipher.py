from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
import base64


class DataCipher:
    """
    DataCipher handles the encryption and decryption of data using AES-GCM.
    """

    def __init__(self, hex_key: str) -> None:
        """
        Initializes the DataCipher instance.

        Args:
            hex_key (str): The secret key in hex format.
        """
        self.secret_key = self.hex_to_bytes(hex_key)

    @staticmethod
    def hex_to_bytes(hex_key: str) -> bytes:
        """Converts a hex string to bytes.

        Args:
            hex_key (str): The hex string.

        Returns:
            bytes: The corresponding bytes representation.
        """
        return bytes.fromhex(hex_key)

    def encrypt_data(self, encryption_value: str) -> str:
        """
        Encrypts the given value using AES-GCM and returns the Base64-encoded result.

        Args:
            encryption_value (str): The value to encrypt.

        Returns:
            str: The Base64-encoded encrypted value.
        """
        nonce = get_random_bytes(16)
        cipher = AES.new(self.secret_key, AES.MODE_GCM, nonce=nonce)
        encrypted_value, tag = cipher.encrypt_and_digest(str(encryption_value).encode())
        return base64.b64encode(nonce + encrypted_value + tag).decode()

    def decrypt_data(self, encrypted_data: str) -> str:
        """
        Decrypts the given encrypted value using AES-GCM and returns the original value.

        Args:
            encrypted_data (str): The Base64-encoded encrypted value.

        Returns:
            str: The decrypted original value.
        """
        combined_value = base64.b64decode(encrypted_data.encode())
        nonce = combined_value[:16]
        encrypted_value = combined_value[16:-16]
        tag = combined_value[-16:]

        cipher = AES.new(self.secret_key, AES.MODE_GCM, nonce=nonce)
        decrypted_value = cipher.decrypt(encrypted_value)

        try:
            cipher.verify(tag)
            return decrypted_value.decode()
        except ValueError:
            raise ValueError("Decryption Error: Tag verification failed")
