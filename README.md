# hadoop-projects

Add the soc-LiveJournal1Adj.txt and the userdata.txt file to hdfs.
Export jar files from the projects and run them using the following commands.

Use the following commands to run the jar files :

hadoop jar Part1.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfriendsout

hadoop jar Part2.jar MutualFriendsCount MutualFriendsCount /user/soc-LiveJournal1Adj.txt /user/mfc1 /user/mfc2

hadoop jar Part3.jar MutualFriendsInformation MutualFriendsInformation /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt

hadoop jar Part4.jar MutualFriends MutualFriends /user/soc-LiveJournal1Adj.txt /user/mfc /user/userdata.txt /user/finaloutput
