# hadoop-projects

Add the soc-LiveJournal1Adj.txt and the userdata.txt file to hdfs.
Export jar files from the projects and run them using the following commands.

Input:
Input files
1. soc-LiveJournal1Adj.txt<br/>
The input contains the adjacency list and has multiple lines in the following format:<br/>
<User><TAB><Friends><br/>
<User> is a unique integer ID(userid) corresponding to a unique user.<br/>
 
2. userdata.txt<br/>
The userdata.txt contains dummy data which consist of<br/>
column1 : userid (<User>) <br/>
column2 : firstname<br/>
column3 : lastname<br/>
column4 : address<br/>
column5: city<br/>
column6 :state<br/>
column7 : zipcode<br/>
column8 :country<br/>
column9 :username<br/>
column10 : date of birth.<br/>
  
#### Program 1: MapReduce program in Hadoop to implements a simple "Mutual/Common friend list of two friends". This program will find the mutual friends between two friends.<br/>

##### Logic : <br/>
Let's take an example of friend list of A, B and C. <br/>

Friends of A are B, C, D, E, F. <br/>
Friends of B are A, C, F. <br/>
Friends of C are A, B, E <br/>
So A and B have C, F as their mutual friends. A and C have B, E as their mutual friends. B and C have only A as their mutual friend. <br/>

##### Map Phase :
In map phase we need to split the friend list of each user and create pair with each friend. <br/>

Let's process A's friend list <br/>
(Friends of A are B, C, D, E , F) <br/>
Key | Value <br/>
A,B | B, C, D, E, F <br/>
A,C | B, C, D, E, F <br/> 
A,D | B, C, D, E, F <br/>
A,E | B, C, D, E, F <br/>
A,F | B, C, D, E, F

Let's process B's friend list <br/>
(Friends of B are A, C, F) <br/>
Key | Value <br/>
A,B | A, C, F <br/>
B,C | A, C, F <br/>
B,F | A, C, F <br/>
We have created pair of B with each of it's friends and sorted it alphabetically. So, the first key (B,A) will become (A,B).

##### Reducer Phase : 
After map phase is shuffling data item into group by key. Same keys go to the same reducer. <br/>
A,B | B, C, D, E, F <br/>
A,B | A, C, F <br/>

Shuffling into {A,B} group and sent to the same reducer. <br/>
A,B | {B, C, D, E , F}, {A, C, F} <br/>

So, finally at the reducer we have 2 lists corresponding to 2 people. Now, we need to find the intersection to get the mutual friends. <br/>

##### Optimization 
To optimize the solution i.e. to make the intersection faster I have used similar concept as merge operation in merge sort. 
I have sorted the friend list in the map phase. So, in reducer side we get 2 sorted lists. This way we can use the merge like operation to take only the matching values instead of going for all possible combinations in O(N2).

Please, make sure that the keys are sorted alphabetically so that we get friends list for 2 person on the same reducer.

##### Output 
The program will output the mutual friends for following pairs. <br/>
(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)<br/>

The code can be easily changed to find mutual friends between all the people by removing the loop which is checking for these keys given above. 

<User_A>,<User_B><TAB><Mutual/Common Friend List><br/>
where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A and B are friends). <br/>
< Mutual/Common Friend List > is a comma-separated list of unique IDs corresponding to mutual friend list of User A and B.<br/>

Code : [MutualFriends](https://github.com/add1993/hadoop-projects/tree/master/MutualFriends)
 
#### Program 2: Find friend pairs whose number of common friends (number of mutual friends) is within the top-10 in all the pairs. Output the output in decreasing order.

Used two pair of map reduce jobs. <br\>
The first map reduce job will find the mutual friends and produce the output with the friends pair and their mutual friends.<br/>
The second map reduce job will read the previous job output and then send the result to the same reducer by using the constant key. The value from mapper in this phase will have <friendPair><tab><mutualFriendList> format and we will directly send this complete line to the reducer and process it there. <br/>
 In the second reducer, we will split the received value by first <tab> and we will get the first value as the friend pair and the second value as a comma separated mutual friend list.<br/>
 We can then again split the mutual friends list and store the count in a java map and use custom comparator to sort the map. Once the map is sorted in descending order we can take the top 10 values. 

##### Output Format:
<User_A>, <User_B><TAB><Number of Mutual Friends><TAB><Mutual/Common Friend Number><br/>
 
 Code : [MutualFriendsCount](https://github.com/add1993/hadoop-projects/tree/master/MutualFriendsCount)
 
#### Program 3: Given any two Users (they are friend) as input, output the list of the names and the city of their mutual friends.
We need to use the userdata.txt to get the extra user information and in memory join to get the required details. 
So, the idea is to load userdata.txt dataset into memory in every mapper, using a hash map data structure to facilitate random access to tuples based on the join key (userid). For this purpose, you can override the method setup (mapper initialization) inside the Map class and load the hash map there inside.

##### Output format:
UserA id, UserB id, list of [city] of their mutual Friends.<br/>

##### Sample Output:
0, 41 [Evangeline: Loveland, Agnes: Marietta]<br/>

Code : [MutualFriendsInformation](https://github.com/add1993/hadoop-projects/tree/master/MutualFriendsInformation)

#### Program 4 : Calculate lowest average age of the direct friends of the users and output the lowest 15.
Step 1: Calculate the average age of the direct friends of each user.<br/>
Step 2: Sort the users by the average age from step 1 in descending order.<br/>
Step 3. Output the tail 15 (15 lowest averages) users from step 2 with their address and the calculated average age.<br/>
We need to use reduce side join.

Code : [MutualFriendsAverageAge](https://github.com/add1993/hadoop-projects/tree/master/MutualFriendsAverageAge)

##### Use the following commands to run the jar files :

hadoop jar Part1.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfriendsout

hadoop jar Part2.jar MutualFriendsCount MutualFriendsCount /user/soc-LiveJournal1Adj.txt /user/mfc1 /user/mfc2

hadoop jar Part3.jar MutualFriendsInformation MutualFriendsInformation /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt

hadoop jar Part4.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt /user/finaloutput
