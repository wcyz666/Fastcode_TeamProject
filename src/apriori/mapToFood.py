__author__ = 'zhengyiwang'

foodfile = open("ingredient_number.txt")
map = {}
for line in foodfile:
    food, num = line.strip().split('\t')
    map[num] = food

f= open("top10recipe")
for line in f:
    nums = line.split()
    food = ""
    for n in nums[:-1]:
        food += map[n] + " "
    print food

