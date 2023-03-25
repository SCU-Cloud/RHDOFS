# RHDOFS: A Distributed Online Algorithm Towards Scalable Streaming Feature Selection
Rough Hypercuboid based Distributed Online Feature Selection (RHDOFS) method is introduced to tackle two critical challenges of Volume and Velocity associated with big data. By fully exploiting the explicit patterns contained in the positive region and the useful implicit patterns derived from the boundary region of rough hypercuboid model, the online relevance selection and online redundancy elimination stages are designed respectively to filter out low-relevance features and keep the redundancy in the selected subset as small as possible for streaming feature scenario. The distributed implementation based on the Apacke Spark platform can not only efficiently handle massive data, but also effectively scale with the cluster and data size.

## FlowChart

<img width="878" alt="image" src="https://user-images.githubusercontent.com/51937754/227576825-33c7db30-417f-4843-8928-7afad5fc91d5.png">

There are three main parts:
* DataSource generates generates a feature data every time. $f_t$ is the $t$-th feature at time $t$.
* RHDOFS contains two stages:
    * Online relevance selection (orange rectangles). 1) If the relevance of feature $f_t$ is less than the relevance threshold $\theta$, it will be filtered out. 2) Else, if the significance of feature $f_t$ is greater than 0, it will be selected.
    * Online redundancy elimination (golden rectangles). When the significance of feature $f_t$ is equal to 0, it will get to this stage. A subset $B_t$ with maximal cardinality of the selected feature $S_{t-1}$ is calculated by greedy algorithm based on this criterion that it can be replaced by $f_t$ and the significance of $S_{t-1}$ is unchanged. 
        * $|B_{t}|$=0. $f_t$ is discarded.
        * $|B_{t}|$=1. Add or discard $f_t$ randomly.
        * $|B_{t}|\gt$1. Discard $B_{t}$ and add $f_t$.
* Distributed Memory allows the intermediate results cached on the cluster and help improve computational efficiency。

## Instructions

### Datasets
[Microarray Datasets](https://csse.szu.edu.cn/staff/zhuzx/Datasets.html)

[UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/datasets.php)

[LIBSVM Datasets](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/)


### RHOFS
RHOFS.m is the sequential version of RHDOFS, implemented as a function on Matlab.
- Input: `X` is a matrix with one row representing an object and one column representing a feature；`Y` is a vector representing the classes.
- Output: `selectedFeatures` is the selected feature set; `time` is the running time.

### RHDOFS
RHDOFS.scala implements a Main function using Scala programing language, whitch contains the calculation process of the entire RHDOFS algorithm. 

The organization of input files:

<img width="440" alt="image" src="https://user-images.githubusercontent.com/51937754/227605662-add4646d-b506-4e14-9e2d-03a428d9c749.png">

* the context of feature data in `part-00000` like:
    ```txt
    1,0.4231,0
    2,0.2638,1
    3,0.5238,1
    ```
    The format is `[no. of object],[feature data],[class]`
* the context of class data in `part-00000` like:
    ```txt
    1,0
    2,1
    3,1
    ```
    The format is `[no. of object],[class]`

The steps to use are as follows:
1. New a maven project, add pom information in pom.xml and fill with the spark and scala version you selected.
    ```xml
    <properties>
        <spark.version>2.x.x</spark.version>
        <scala.version>2.11</scala.version>
    </properties> 
    ```
2. Add RHDOFS.scala file to your source directory.
3. Compile it into a jar package and upload it to the spark cluster。
4. Use spark command to submit your job.
    ```shell
    ./bin/spark-submit \
    --class RHDOFS \
    --master <master-url> \
    --deploy-mode <deploy-mode> \
    --conf <key>=<value> \
    ... # other options
    <application-jar> \
    [application-arguments]
    ```
    There are four input arguments: 1) the input file path. 2) the number of features. 3) the number of class. 4) the partition number of RDD.
