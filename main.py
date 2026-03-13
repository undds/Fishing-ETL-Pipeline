from collections import Counter
print(1)
print("hello")
# # Pull even and odd numbers from a list
# nums = [1,2,3,4,5,8,0,22,7]
# even = [n for n in nums if n %2 ==0]
# odd = [n for n in nums if n %2 !=0]
# secondLarges = sorted(nums)[-1]
# print(secondLarges)
#Count frequency of words in a string
#Use Counter from Python collections.
words = "Hello my name is is name"
freq = Counter(words.split())
print(freq)