# MP4



## Getting started
First, we need to clone this project and get into the shell package,
make sure you have cloned the repo on all the VMs
```
git clone <gitlab_URL>
cd ./mp4/src/main/java
```

## start Servers
We can use the Server.sh to start our project
```
./Server.sh
```
Then We will see a hint read "Please input your command:"

we support the following commands(ignore case):

"Inference <input_files> <model>" will start inference the files

"C1 <job_num>" will show query rate of two jobs, the total number of queries processed

"C2" will show average time, standard deviation, median and percentile

"C3 <job_num> <batch_size>" will set batch size

"C4 <job_num>" will show the result of inference

"C5" will show the allocation of jobs

To better manipulate the server, we recommend open all the VMs in terminal, so that we can use commands on every machine.