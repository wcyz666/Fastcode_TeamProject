__author__ = 'zhengyiwang'

'''
duplicate data set to be 10 times bigger
'''

for i in range(1,11):
    fread =  open("data.dat")
    f = open("data"+ str(i)+".dat", 'w')
    for line in fread:
        nums = line.strip().split(' ')
        newnums = [str(int(x) + 1000 * i) for x in nums]
        newline = ' '.join(newnums) + '\n'
        f.write(newline)
