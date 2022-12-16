ip=("vmHostnames...")
username=$1
for element in ${ip[*]}
do
ssh -t $username@$element "cd ./mp4/src/main/java/utils; sed -i 's/xx../xxxx/g' master.txt"
done
