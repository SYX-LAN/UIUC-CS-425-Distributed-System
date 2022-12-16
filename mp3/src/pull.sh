ip=("vmHostnames...")
username=$1
for element in ${ip[*]}
do
ssh -t $username@$element "cd ./mp3; git pull origin main"
done
