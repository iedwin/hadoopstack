# coding=utf8
# 字典

d2 = dict
d2={'master':201,'slave1':202,'slave2':203,'slave3':204}

print d2
print d2.get('tang') ##print d['tang']

print d2.has_key('tang')
print d2.has_key('master')
print d2.items()
print d2.keys()
print d2.values()
print d2.popitem()
print d2
print d2.pop("slave2")
print d2

d={'tom','jason','kkk','yu'}
print d

d3=set([1,2,3,6,7,9])
print d3

d4=(99,88,77,66,55)
print d4