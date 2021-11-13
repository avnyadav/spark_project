## Spark Configuration in window 10

1. Downlaod all required file from below URL:
```
https://drive.google.com/drive/folders/1rBauyUVCRTbnKXgkMGh4l9MdIOVj8CQc?usp=sharing
```

2. Install java .exe file

3. Extract spark file in C drive

4. Extract kafka file in C drive

5. Add environment variable 
 
<TABLE border="1">
<tr>
<th>ENVIRONMENT VARIABLE NAME</th>
<th>VALUE</th>
</tr>
<tr>
<td>HADOOP_HOME</td>
<td>C:\winutils</td>
</tr>
<tr>
<td>JAVA_HOME</td>
<td>C:\Program Files\Java\jdk1.8.0_202</td>
</tr>
<tr>
<td>SPARK_HOME</td>
<td>C:\spark-3.0.3-bin-hadoop2.7</td>
</tr>
</TABLE>

6. select path  variable from environment variable and add below values.
```buildoutcfg
%SPARK_HOME%\bin
```
```buildoutcfg
%HADOOP_HOME%\bin
```
```buildoutcfg
%JAVA_HOME%\bin
```

## Create conda environment 

1. open conda terminal execute below command

```buildoutcfg
conda create -n <env_name> python=3.8 -y
```

2. select <env_name> created in previous step for project interpreter in pycharm.

3. Install all necessary python library specified in requirements.txt file using below command.
```buildoutcfg
pip install -r requirements.txt
```


5. To upload your code to gihub repo
```
git init
git add .
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/Avnish327030/spark_project.git
git push -u origin main
```