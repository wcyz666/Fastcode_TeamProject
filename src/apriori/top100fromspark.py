__author__ = 'zhengyiwang'



'''
get top 100 from output, sorted by count
'''

from operator import itemgetter

import sys

f= open("top100", 'a')
array = []
for line in sys.stdin:
    line = line.replace('(', '').replace(')','')
    nums = line.strip().split(',')
    nums[-1] = int(nums[-1])
    array.append(nums)
array.sort(key = itemgetter(3),reverse = True)

for i in range(100):

    print ' '.join(array[i][:3]), array[i][-1]
