import datetime

s = []
FILE = open("triangle.txt", "r")
for blob in FILE: s.append([int(i) for i in blob.split(" ")])


def sum(s,r,c):
    if(r<len(s) and c>=0 and c<len(s[r])):
        sm=[]
        sm.append(sum(s,r+1,c)+s[r][c])
        sm.append(sum(s,r+1,c+1)+s[r][c])
        return max(sm)
    return 0

print(datetime.datetime.now())
print(sum(s,0,0))
print(datetime.datetime.now())