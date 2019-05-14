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
  
Program 1: MapReduce program in Hadoop to implements a simple "Mutual/Common friend list of two friends". This program will find the mutual friends between two friends.

Use the following commands to run the jar files :

hadoop jar Part1.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfriendsout

hadoop jar Part2.jar MutualFriendsCount MutualFriendsCount /user/soc-LiveJournal1Adj.txt /user/mfc1 /user/mfc2

hadoop jar Part3.jar MutualFriendsInformation MutualFriendsInformation /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt

hadoop jar Part4.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt /user/finaloutput
