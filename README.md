## Spark Configuration in window 10

1. Downlaod all required file from below URL:
```
https://drive.google.com/drive/folders/1rBauyUVCRTbnKXgkMGh4l9MdIOVj8CQc?usp=sharing
```

2. Install java .exe file
> note: choose installtion path of java to "C:" drive

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
<td>C:\Java\jdk1.8.0_202</td>
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
```buildoutcfg
C:\Java\jre1.8.0_281\bin
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
git remote add origin <github_repo_link>
git push -u origin main
```

## Train random forest model on insurance dataset
```buildoutcfg
python training\stage_00_data_loader.py
```
```buildoutcfg
python training\stage_01_data_validator.py
```
```buildoutcfg
python training\stage_02_data_transformer.py
```
```buildoutcfg
python training\stage_03_data_exporter.py
```
```buildoutcfg
spark-submit training\stage_04_model_trainer.py
```

## Prediction using random forest of insurance dataset
```buildoutcfg
python prediction\stage_00_data_loader.py
```
```buildoutcfg
python prediction\stage_01_data_validator.py
```
```buildoutcfg
python prediction\stage_02_data_transformer.py
```
```buildoutcfg
python prediction\stage_03_data_exporter.py
```
```buildoutcfg
spark-submit prediction\stage_04_model_predictor.py
```



# start zookeeper and kafka server



## start kafka producer using below command
```buildoutcfg
spark-submit csv_to_kafka.py
```

## start pyspark consumer using below command
```buildoutcfg
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  spark_consumer_from_kafka.py
```