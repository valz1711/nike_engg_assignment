# nike_engg_assignment

Steps to deploy the job:
1. Import the scala project nike_de in your Scala IDE.
2. Build the jar by running the imported project as maven install.
3. Move the built jar to the desired folder in our cluster.
4. Execute the below spark-submits accordingly.

**Local mode - executed and working**

spark-submit --class com.cts.nike.file.proc.utils.UniqueJSONWrite --master local --name CSV_TO_UNIQUE_JSONS /mnt/c/IDW/SparkJobs/nike_de-0.0.1-SNAPSHOT.jar /mnt/c/IDW/Inputs/Nike/dataengineertest/dataengineertest/ /mnt/c/IDW/Outputs/JSONExplodeSHELL

**yarn-cluster mode - alter paths to jar/input/output files**

spark-submit --driver-memory 2G --executor-memory 2G --driver-cores 2 --executor-cores 2 --num-executors 2 --master yarn-cluster --name CSV_TO_UNIQUE_JSONS --jars /path/to/supporting_jar1.jar,/path/to/supporting_jar2.jar /path/to/nike_de-0.0.1-SNAPSHOT.jar /path/to/input/files/ /path/to/output/directory 
