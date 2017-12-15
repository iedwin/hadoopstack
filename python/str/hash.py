# -*- coding:utf-8 -*-
def convert_n_bytes(n, b):
    bits = b*8
    return (n + 2**(bits-1)) % 2**bits - 2**(bits-1)

def convert_4_bytes(n):
    return convert_n_bytes(n, 4)

def getHashCode(s):
    h = 0
    n = len(s)
    for i, c in enumerate(s):
        h = h + ord(c)*31**(n-1-i)
    return convert_4_bytes(h)

if __name__ == '__main__':
    print getHashCode('http://outofmemory.cn/')
    print getHashCode('http://outofmemory.cn/code-snippet/2311/C-rumenjiaocheng-c-multithreading-process-course')
    print getHashCode('http://outofmemory.cn/code-snippet/2321/C-rumenjiaocheng-usage-arrow-unsafe-code-block/')
    print getHashCode('http://outofmemory.cn/code-snippet/2322/mysql-achieve-sql-server-with-lock')
    print getHashCode('http://outofmemory.cn/')
    print getHashCode('http://outofmemory.cn/code-snippet/2324/java-unit-test-usage-jMockit-mock-jingtailei')
    print getHashCode('88')