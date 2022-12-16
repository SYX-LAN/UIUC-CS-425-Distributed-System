ip=("vmHostnames...")
username=$1
for element in ${ip[*]}
do
ssh $username@$element "pkill -f 'java.*Server'; pkill -f 'java.*MasterIndicator'"
done
echo "Servers shutdown"
