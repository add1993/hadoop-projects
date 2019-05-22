# hadoop-projects

Add the soc-LiveJournal1Adj.txt and the userdata.txt file to hdfs.
Export jar files from the projects and run them using the following commands.

Input:
Input files
1. soc-LiveJournal1Adj.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
<User> is a unique integer ID(userid) corresponding to a unique user.
 
2. userdata.txt
The userdata.txt contains dummy data which consist of
column1 : userid (<User>)
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.
  
#### Program 1: MapReduce program in Hadoop to implements a simple "Mutual/Common friend list of two friends". This program will find the mutual friends between two friends.

##### Logic : 
Let's take an example of friend list of A, B and C.

Friends of A are B, C, D, E, F.
Friends of B are A, C, F.
Friends of C are A, B, E
So A and B have C, F as their mutual friends. A and C have B, E as their mutual friends. B and C have only A as their mutual friend.

##### Map Phase :
In map phase we need to split the friend list of each user and create pair with each friend.

Let's process A's friend list
(Friends of A are B, C, D, E , F) 
Key | Value
A,B | B, C, D, E, F 
A,C | B, C, D, E, F 
A,D | B, C, D, E, F 
A,E | B, C, D, E, F 
A,F | B, C, D, E, F

Let's process B's friend list
(Friends of B are A, C, F)
Key | Value
A,B | A, C, F
B,C | A, C, F
B,F | A, C, F
We have created pair of B with each of it's friends and sorted it alphabetically. So, the first key (B,A) will become (A,B).


##### Reducer Phase : 
After map phase is shuffling data item into group by key. Same keys go to the same reducer.
A,B | B, C, D, E , F
A,B | A, C, F

Shuffling into {A,B} group and sent to the same reducer.
A,B | {B, C, D, E , F}, {A, C, F}

So, finally at the reducer we have 2 lists corresponding to 2 people. Now, we need to find the intersection to get the mutual friends. 

##### Optimization 
To optimize the solution i.e. to make the intersection faster I have used similar concept as merge operation in merge sort. 
I have sorted the friend list in the map phase. So, in reducer side we get 2 sorted lists. This way we can use the merge like operation to take only the matching values instead of going for all possible combinations in O(N2).

Please, make sure that the keys are sorted alphabetically so that we get friends list for 2 person on the same reducer.

 Code : [MutualFriends](https://github.com/add1993/hadoop-projects/tree/master/MutualFriends)
 
 
 

##### Use the following commands to run the jar files :

hadoop jar Part1.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfriendsout

hadoop jar Part2.jar MutualFriendsCount MutualFriendsCount /user/soc-LiveJournal1Adj.txt /user/mfc1 /user/mfc2

hadoop jar Part3.jar MutualFriendsInformation MutualFriendsInformation /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt

hadoop jar Part4.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt /user/finaloutput
