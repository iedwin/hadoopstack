import hashlib

if __name__ == '__main__':
    src = '12345'
    m2 = hashlib.md5()
    m2.update(src)
    print m2.hexdigest()
