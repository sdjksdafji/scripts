import argparse
import subprocess
from time import sleep
import sys

jobTerminated = False
jobId = "76f83e8f-4ea5-49da-a2b3-fdf428d90055"
clusterName = "friend-graph-dataproc"

while not jobTerminated:
  out = subprocess.getoutput("gcloud dataproc jobs describe " + jobId)
  for rawMsg in out.split("\n"):
    msg = rawMsg.strip()
    #print(msg)
    if "state:" in msg and (not "state: PENDING" in msg) and (not "state: RUNNING" in msg) and (not "state: SETUP_DONE" in msg):
      print(msg)
      jobTerminated = True
      break;
  print("Waiting for job to finish ...")
  sys.stdout.flush()
  sleep(5)
print("Job finished. Ready to delete cluster. Waiting 5 min inorder to wait for eventual consistency")
sleep(5 * 60)
deleteClusterOutput = subprocess.getoutput("echo Y | gcloud dataproc clusters delete " + clusterName)
print(deleteClusterOutput)
print("Cluster Deleted. Exiting Script")
