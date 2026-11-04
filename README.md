wget -qO ~/hadoop-lab.sh https://raw.githubusercontent.com/ragav2730/hadoop-lab.sh/main/hadoop-lab.sh && sed -i 's/\r$//' ~/hadoop-lab.sh && bash ~/hadoop-lab.sh && source ~/.bashrc

to run second program there is a file in downloads/file1.txt (you can alter as per your wish) then run this command in your terminal for second program output
hdfs dfs -cat /output1/*


lines = LOAD '/user/cloudera/HDFSFILE.txt' AS (line:chararray);

words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

grouped = GROUP words BY word;

wordcount = FOREACH grouped GENERATE group, COUNT(words);

DUMP wordcount;



# 1. Create input text file
nano HDFSFILE.txt

# 2. Create Pig program
nano pigexp.pig

# 3. Edit Pig program (if required)
nano pigexp.pig

# 4. Create directory in HDFS
hdfs dfs -mkdir -p /user/cloudera

# 5. Upload text file to HDFS
hdfs dfs -put HDFSFILE.txt /user/cloudera

# 6. Execute Pig program
pig pigexp.pig




CREATE DATABASE stu;

USE stu;

CREATE TABLE stu2 (
    sid INT,
    name STRING,
    gpa FLOAT
);

INSERT INTO TABLE stu2 VALUES
(1, 'Raja', 5.0),
(2, 'Kumar', 8.5),
(3, 'Ravi', 9.0);

SHOW DATABASES;

SELECT * FROM stu2;








