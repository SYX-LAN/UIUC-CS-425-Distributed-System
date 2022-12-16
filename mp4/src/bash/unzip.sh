cd ..
cd main
cd java
cd SDFS_files
cd weather
zip_array=(a.zip b.zip test.zip)
for(( i=0;i<${#zip_array[@]};i++)) 
 do
    unzip ${#zip_array[i]}
    cd test
    mv * ../
    cd ..
    rm -rf test
    rm -rf ${#zip_array[i]}
done;
