# coding=utf8
# 数据类型
print  type (1)
print type ("str")
print type (True)
print type ('0.01')
isB = True
if isB == True:
    print "isB is ",'True'
else :
    print 'False'

# 数组
l = [1,2,3,4,8,5]
print l
print type(l)

# 元祖
t = ('java','scala','python')
print t

# list高级
l2 = [5,6,7]
l3 = l.extend(l2) ## l2添加到l
print l
print l2
print l3

print l.count(5)
print l.reverse()
print l
print l.index(5)
print l.index(5,1,70)

