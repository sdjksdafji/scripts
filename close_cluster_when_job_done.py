import argparse
import subprocess
from time import sleep
import sys

parser = argparse.ArgumentParser()
parser.add_argument("job_id", help="Job ID from Google Cloud Dataproc Jobs")
parser.add_argument("--cluster_name", default="friend-graph-dataproc",
                    help="Cluster Name from Google Cloud Dataproc Jobs")
parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")
args = parser.parse_args()

jobId = args.job_id
clusterName = args.cluster_name

print("Trying to monitor job with ID: " + jobId)
print("On cluster with name: " + clusterName)

jobTerminated = False
counter = 0

while not jobTerminated:
    out = subprocess.getoutput("gcloud dataproc jobs describe " + jobId)
    if args.verbose:
        print(out)
    for rawMsg in out.split("\n"):
        msg = rawMsg.strip()

        if "state:" in msg and (not "state: PENDING" in msg) and (not "state: RUNNING" in msg) and (
                not "state: SETUP_DONE" in msg):
            print(msg)
            jobTerminated = True
            break;

    if not jobTerminated:
        print("\r" + "Waiting for job to finish " + ("." * (counter + 1)) + "   ", end="\r")
        sys.stdout.flush()
        sleep(5)
    counter += 1
    counter %= 3

print("Job finished. Ready to delete cluster. Waiting 5 min inorder to wait for eventual consistency")
sleep(5 * 60)
deleteClusterOutput = subprocess.getoutput("echo Y | gcloud dataproc clusters delete " + clusterName)
print(deleteClusterOutput)
print("Cluster Deleted. Exiting Script")
