ip=("vmHostnames...")
username=$1
for element in ${ip[*]}
do
ssh $username@$element "cd ./mp3/src; ./Process.sh 1>/dev/null 2>/dev/null &"
done
echo Process started