ip=("vmHostname...")
username=$1
for element in ${ip[*]}
do
ssh -t $username@$element "cd ./mp2; git pull origin main"
done
