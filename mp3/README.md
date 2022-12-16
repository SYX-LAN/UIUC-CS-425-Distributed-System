# MP3



## Getting started
First, we need to clone this project and get into the shell package, 
make sure you have cloned the repo on all the VMs
```
git clone <gitlab_URL>
cd ./mp3/src
```
## start a separate Process
We can use the Start.sh to start this separate process, this process can tell our VMs which machine is the master
```
./Start.sh
```

## start Servers
We can use the Server.sh to start our project
```
./Server.sh
```
Then We will see a hint read "Please input your command:"
we support the following commands(ignore case):
1. "Leave" for a node leaves the group
2. "Join" for a node joins the group
3. "Print" for a node shows its membershipList
4. "Store" for a node shows all files stored on it.
5. "LS <filename>" to show where is the file stored
6. "Put <localFile> <SdfsFile>" to put localFile on Sdfs
7. "Get <SdfsFile> <localFile>" to get the latest version from Sdfs, which will be named as localFile
8. "Delete <SdfsFile>" to delete all versions of the file on Sdfs
9. "get-versions <sdfsfilename> <numversions> <localfilename>"  to get last numversions of sdfsfile to localfile, and these different versions will use delimiters to mark out.

To better manipulate the server, we recommend open all the VMs in terminal, so that we can use commands on every machine.
