# CS553CourseProject
**Retrieving Input Data**<br/><br/>
Input data can be retrieved by running *getmatchdata.py*. Note that the API endpoint from which the data is retrieved has a rate limit of 2000 calls per day and 60 calls per minute. In order to retrieve all of the data, it may be required to run the script multiple times across multiple days. A sample of the input data is included at the *matches* directory. For the benchmark programs to run

**Azure Spark Setup**<br/><br/>
Three VMs must be created in Azure. I used Standard D2s v3 (2 vCPUs, 8 GB memory) VMs. Run *setup.sh* to install the required packages (including Spark) and create the necessary directories on the VMs. The machines must be able to connect and network with each other. This requires placing them in the same virtual network in Azure, as well as adding the machines' IP addresses to each other's */etc/hosts/*. <br/><br/>

A requirement for Spark is for the machines to have a shared NFS disk. I configured one by following the instructions here: https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux <br/>
In my project, the name of this disk is */mount/isnfs/qfileshare*. It must be mounted at the same location on each of the VMs. It is required for the input data *matches* directory to be located in the NFS disk. It is also required for an *output* directory to exist in the NFS disk.<br/><br/>

Once the above setup is complete, it should be possible to ssh into each of the VMs. Spark should be installed on each VM. One of the VMs will act as the master node. Spark include a script */sbin/start-master.sh/* to start the master node. This script will output the address (machine where the script is started) and port (default: 7077) where the master runs. On each of the worker machines, run the included Spark script */sbin/start-worker.sh/* with the name of the master from the previous step. Once this is complete, a Spark Standalone cluster should have been successfully deployed. <br/><br/>

To submit a job (in this case the benchmark program) to the cluster, run *./bin/spark-submit --master {address of master} sparkdriver.py*. It will take around 30 minutes for the benchmark program to complete. Running jobs can be monitored at port 4040 of the master machine. Completed jobs can be monitored at port 18080 of the master machine, as long as the history server is running. The history server can be started by running *./sbin/start-history-server.sh*. Metrics can be collected by running *metrics.py* with the correct app ID of the submitted job. This script will take a few minutes to complete and will print several metrics to terminal, as well as produce graphs summarizing latency informatio. <br/><br>

**Google Dataflow Setup**<br/><br/>

A new Google Cloud project called CS553 must be created with the following APIs enabled:
* Dataflow API
* Compute Engine API
* Cloud Logging API
* Cloud Storage
* Google Cloud Storage JSON API
* BigQuery API
* Cloud Pub/Sub API
* Cloud Datastore API
* Cloud Resource Manager API

<br/>In the Google Cloud console, the 259367267444-compute@developer.gserviceaccount.com account must be granted Storage Object Admin, Dataflow Admin, and Dataflow Worker roles in the CS553 project.<br/>
In the Cloud Shell, the following commands must be run to install Apache Beam (requirement for Google Dataflow):
* pip3 install virtualenv
* python3 -m virtualenv env
* source env/bin/activate
* pip3 install apache-beam[gcp]

<br/>In the Google Cloud console, a new bucket with the name *dataflow-cs553-459018* must be created. The input data *matches* directory must be uploaded to this bucket.<br/>
Once all of the above setup is complete, the benchmark program can be run with the following command: <br/>
`python3 sparkdriver.py --region us-central1 --input gs://dataflow-cs553-459018/matches/ --output gs://dataflow-cs553-459018/results/ --runner DataflowRunner --project cs553-459018 --temp_location gs://dataflow-cs553-459018/temp/ --num_workers 2 --machine_type n1-standard-2`
