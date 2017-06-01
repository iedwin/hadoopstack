import Person

try:
    print 'tyr .....'

    zs = Person
    zs.say('zs')


    r = 10/0
except Exception as e:
    print 'exception ...'
    print e.message

finally:
    print 'end ...'



