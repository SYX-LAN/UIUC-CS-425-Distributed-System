ip=("vmHostname...")
username=$1
for element in ${ip[*]}
do
ssh $username@$element "cd ./mp1/src/main/java/shell; ./Server.sh 1>/dev/null 2>/dev/null &"
done
echo Servers connected
