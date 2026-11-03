wget -qO ~/hadoop-lab.sh https://raw.githubusercontent.com/ragav2730/hadoop-lab.sh/main/hadoop-lab.sh && sed -i 's/\r$//' ~/hadoop-lab.sh && bash ~/hadoop-lab.sh && source ~/.bashrc

to run second program there is a file in downloads/file1.txt (you can alter as per your wish) then run this command in your terminal for second program output
hdfs dfs -cat /output1/*


