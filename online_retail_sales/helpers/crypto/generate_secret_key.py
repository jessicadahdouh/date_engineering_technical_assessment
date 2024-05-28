from Crypto.Random import get_random_bytes


def generate_key_hex():
    """Generates a random 256-bit key and returns its hexadecimal representation.

    Returns:
        str: The hexadecimal representation of the generated random key.
    """
    secret_key = get_random_bytes(32)  # 256-bit key as a binary string
    return secret_key.hex()


print("Generated secret key:", generate_key_hex())



