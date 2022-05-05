# Kmeans Algorithm using Map Reduce

In the big data era, most service provider choose to store web data, app data, and sensor data in distributed storage system to reduce the cost. In this repo, we discuss using hadoop mapreduce to aplly Kmeans algorithm on data stored in hadoop distributed file system (hdfs).

## Method

This method is composed of three main parts: Mapper, Reducer, and Loop. The Loop calls Mapper and Reducer to calculate the centers of the dataset until reaching the stopping cretira.

![alt text](https://github.com/Yuliang795/KMeans_MapReduce/blob/main/logistics_img/workflow_wbg.png)

### Mapper 

Read the input data file by lines, and the mapper process each line at a time by (1) convert the string line to Point class (2) compare each line with N center points and record the nearest center point (3) return current point and the nearest center points in the format of (center point ID, (x,y)).
For point in file:
    for center_i in all_centers:
        calculate the distance from point to center_i and store the center with shortest distance.

The output data format is (center point ID, (x,y))

### Reducer

Reducer receives organized sets of data points from Mapper, where the data is in the key-value pair format. The key is the center point ID(cluster ID), and the value is the set of data points near it. (center#ID, (xi,yi), (xj,yj))
The reducer finds the new center points by calculating the average of each cluster. (cx1', cy1')=(sum(x), sum(y))/len(xi)

### Loop

Loop is the main class of this method. It is responsible for initiating the job, managing Mapper and Reducer, and checking stopping criteria. The Loop keeps executing MapReduce task until the stopping criteria are reached.

### Initial strategy

Users can provide a self-defined initial strategy. In this demo, the Loop randomly picks N points from the data file as initial center points.

### Stopping criteria 

Users can provide self-defined criteria functions. In this demo, the loop stops if (1) the Euclidean  distance between each new center point and the old center point is less than 1% of the euclidean distance of the old center point OR (2) reaches max iteration number 20



## Prepare data

```bash
# create folder to store the Input data
hdfs dfs -mkdir -p km1
# copy data file from local to hdfs
hdfs dfs -copyFromLocal 'data_points.txt' km1
```

##  Execute Kmeans

```bash
# execute the jar file using hadoop jar
hadoop jar demo.jar km1 output
# check the result in the output file
hdfs dfs -cat /ourput/part-00000
```

