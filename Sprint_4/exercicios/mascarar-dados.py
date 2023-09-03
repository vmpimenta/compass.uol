from hashlib import sha1

while True:
    palavra = (input)("Escreva uma palavra para saber sua hash: ")

    hash_palavra = sha1(palavra.encode())
    print("Hash SHA1 da palavra:", hash_palavra.hexdigest())