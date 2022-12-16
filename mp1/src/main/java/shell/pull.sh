ip=("vmHostname...")
username=$1
for element in ${ip[*]}
do
ssh -t $username@$element "cd ./mp1; git pull origin main"
done
