ip=("vmHostnames...")
username=$1
for element in ${ip[*]}
do
ssh -t $username@$element "cd ./mp4; git pull"
done
