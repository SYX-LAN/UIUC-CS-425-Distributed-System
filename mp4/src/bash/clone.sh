ip=("vmHostnames...")
username=$1
for element in ${ip[*]}
do
ssh -t $username@$element "git clone xxxxx"
done
