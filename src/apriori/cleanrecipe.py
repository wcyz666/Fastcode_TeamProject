__author__ = 'zhengyiwang'

f = open("recipe-n.txt")
ff = open("recipe.txt",'w')
for line in f:
    if line.strip():
        ff.write(line.strip() +'\n' )



