from sys import argv
import linecache
# first arg is the count file, the second is file stands for a int or ints (for total), third is the machine number or 11 (for total)
ix = int(argv[3])
line = linecache.getline(argv[1], ix)
num1 = int(line.split(':')[1])
with open(argv[2]) as f:
	num2 = sum(int(line) for line in f.readlines())
if (num1 != num2):
	print('Failed count for ' + ('total' if ix>10 else 'machine '+str(ix))+': ')
	print('Expected: ',num2,'; Found: ',num1)
