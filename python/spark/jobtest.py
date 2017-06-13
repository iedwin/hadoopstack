import re
if __name__ == '__main__':

    m = re.match(r'(\w+) (\w+)(?P<sign>.*)', 'hello world!')
    print m.groups()
    text = 'var appsTableData=jhhkhkjfhkjhk'
    print text
    m = re.match(r'var appsTableData=(\w+)', text)
    print m.groups()
    if m:
        print m.group(0), '\n', m.group(1)
    else:
        print 'not match'