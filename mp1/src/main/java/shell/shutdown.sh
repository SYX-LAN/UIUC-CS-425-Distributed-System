ip=("vmHostname...")
username=$1
for element in ${ip[*]}
do
ssh $username@$element "pkill -f 'java.*Server'"
done
echo "Servers shutdown"
