def add(x,y,f):
    return f(x)+f(y)

print  add(-4,5,abs)

def show(x,*y):
    return x,y

print show(1,2,3,4,5,6,7,8,9)