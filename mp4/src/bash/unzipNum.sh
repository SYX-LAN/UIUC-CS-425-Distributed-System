cd ..
cd main
cd java
cd SDFS_files
cd weather
zip_array=(a.zip b.zip c.zip d.zip e.zip f.zip)
for(( i=0;i<${#zip_array[@]};i++)) 
 do
    unzip ${#zip_array[i]}
    cd total
    mv * ../
    cd ..
    rm -rf total
    rm -rf ${#zip_array[i]}
done;
