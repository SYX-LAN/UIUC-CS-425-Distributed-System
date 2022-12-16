file_path=$1
stored_file=$2
model_type=$3
index=$4
total=$5
cd ..
cd ..
cd python
python predict.py $file_path $stored_file $model_type $index $total
