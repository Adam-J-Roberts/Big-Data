#!/bin/bash
#Put java file, input txt, and this script in folder BY THEMSELVES(!) and run.

#Get directory,file, and input names
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR
FILE_NAME=`echo *.java`
FILE_NAME=${FILE_NAME::-5}
INPUT_NAME=`echo *.txt`
INPUT_NAME=${INPUT_NAME::-4}

#Delete and recreate any directories, then copying files to locations
rm -r /usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}
mkdir /usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}
mkdir /usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}/$FILE_NAME
cp ${INPUT_NAME}.txt ~/HadoopInput
cp ${FILE_NAME}.java /usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}
cd ~/HadoopInput
mv ${INPUT_NAME}.txt ${FILE_NAME}_input.txt
#Execute creation
cd /usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}
export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
javac -classpath ${HADOOP_CLASSPATH} -d ${FILE_NAME}/ ${FILE_NAME}.java
if [ ! -d "/usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}/$FILE_NAME/org" ]; then
  echo "Could not compile!"
  exit 3
fi
jar -cvf ${FILE_NAME}.jar -C ${FILE_NAME}/ .
cd
rm -r ~/HadoopOutput/${FILE_NAME}output
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/${FILE_NAME,,}/${FILE_NAME}.jar org.apache.hadoop.ramapo.${FILE_NAME} ~/HadoopInput/${FILE_NAME}_input.txt ~/HadoopOutput/${FILE_NAME}output

#Open output file
gedit ~/HadoopOutput/${FILE_NAME}output/part*

#unlock with sudo chattr -i hadoopMake.sh