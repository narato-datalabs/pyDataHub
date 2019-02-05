
getClusterState() {
    local clusters="$(databricks clusters list)"
    local clusterline="$(grep $1 <<< $clusters)"
    local __clusterDef=$2
    eval $__clusterDef="($clusterline)"
}

clusterName=""
packageName="pyDataHub-0.1-py3.7.egg"

while [[ "$1" =~ ^- && ! "$1" == "--" ]]; do case $1 in

  -cn | --clusterName )
    shift; clusterName=$1
    ;;
esac; shift; done


if [[ -z $clusterName ]]; then
    echo "The cluster name was not provided. Add '-cn <clustername> or --clusterName <clusterName>' to specify the cluster name."
    exit 1
fi

getClusterState $clusterName clusterDef

if [[ ${#clusterDef[@]} -eq 0 ]]; then
    echo "The cluster with name '$clusterName' was not found."
    exit 1
fi

clusterId=${clusterDef[0]}
clusterState=${clusterDef[2]}
echo "Cluster definition"
echo "=================="
echo "name : ${clusterName}"
echo "id : ${clusterId}"
echo "state : ${clusterState}"

if [ ${clusterState} == "TERMINATED" ]; then
    echo "Cluster not running. Starting cluster ..."
    databricks clusters start --cluster-id $clusterId
fi

getClusterState $clusterName clusterDef
clusterState=${clusterDef[2]}

while [ $clusterState == "PENDING" ]; do
    sleep 5s
    getClusterState $clusterName clusterDef
    clusterState=${clusterDef[2]}
    echo "Cluster state : ${clusterState}"
done


if [ $clusterState != "RUNNING" ]; then
    echo "Unable to start cluster."
    exit 1
fi

echo "Cluster started succesfully."

echo "Checking the /jars folder"
databricks fs mkdirs dbfs:/FileStore/jars
echo "Deleting possible existing file."
databricks fs rm dbfs:/FileStore/jars/${packageName}
echo "Uploading library package"
databricks fs cp ./dist/${packageName} dbfs:/FileStore/jars
echo "Uninstallingg possible previous library registration on cluster ${clusterName}"
databricks libraries uninstall --cluster-id ${clusterId} --egg dbfs:/FileStore/jars/${packageName}
echo "Restarting cluster"
databricks clusters restart --cluster-id ${clusterId}

getClusterState $clusterName clusterDef
clusterState=${clusterDef[2]}

while [ $clusterState == "RESTARTING" ]; do
    sleep 5s
    getClusterState $clusterName clusterDef
    clusterState=${clusterDef[2]}
    echo "Cluster state : ${clusterState}"
done

echo "Cluster restarted succesfully."

echo "Installing library on cluster ${clusterName}"
databricks libraries install --cluster-id ${clusterId} --egg dbfs:/FileStore/jars/${packageName}
