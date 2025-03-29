import base64
import hashlib

# Function to encrypt (encode) a string using Base64
def base64_encrypt(data):
    encoded_data = base64.b64encode(data.encode('utf-8'))
    return encoded_data.decode('utf-8')

# Function to decrypt (decode) a Base64-encoded string
def base64_decrypt(encoded_data):
    decoded_data = base64.b64decode(encoded_data.encode('utf-8'))
    return decoded_data.decode('utf-8')

# Function to hash a string using SHA-256
def sha256_hash(data):
    sha256_hashed = hashlib.sha256(data.encode('utf-8')).hexdigest()
    return sha256_hashed

# Function to hash a string using SHA-512
def sha512_hash(data):
    sha512_hashed = hashlib.sha512(data.encode('utf-8')).hexdigest()
    return sha512_hashed

# Example usage
if __name__ == "__main__":
    original_string = "Hello, Secure World!"

    # Base64 Encoding and Decoding
    base64_encoded = base64_encrypt(original_string)
    base64_decoded = base64_decrypt(base64_encoded)
    print(f"Original String: {original_string}")
    print(f"Base64 Encoded: {base64_encoded}")
    print(f"Base64 Decoded: {base64_decoded}")

'''
    # SHA-256 Hashing
    sha256_hashed = sha256_hash(original_string)
    print(f"SHA-256 Hashed: {sha256_hashed}")

    # SHA-512 Hashing
    sha512_hashed = sha512_hash(original_string)
    print(f"SHA-512 Hashed: {sha512_hashed}")
    
'''