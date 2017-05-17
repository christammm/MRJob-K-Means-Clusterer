import sys
import numpy as np
import pickle as pk
import matplotlib.pyplot as plt


from kmeans_selector import KMeansCentroidSelector
from kmeans_updater import KMeansUpdateLocation

def getCentroids(job, runner):
    centroids = []
    for line in runner.stream_output():
        key, value = job.parse_output_line(line)
        print  key, value
        centroids.append(value)
    return centroids
    
def writeCentroidsToDisk(centroids, fileName):
    centroidFile = open(fileName, "w")
    pk.dump(centroids,centroidFile)
    centroidFile.close()

def findBiggestDifference(centroids, newCentroids):
    distances = [np.linalg.norm(np.array(coordOne) - coordTwo) for coordOne,coordTwo in zip(centroids,newCentroids)]
    maxDistance = max(distances)
    return maxDistance
    
CENTROIDS_FILE = "centroids.txt"

if __name__ == '__main__':
    args = sys.argv[1:]
    
    selectorJob = KMeansCentroidSelector(args=args)
    with selectorJob.make_runner() as selectorJobRunner:
        selectorJobRunner.run()

        centroids = getCentroids(selectorJob, selectorJobRunner)
        writeCentroidsToDisk(centroids, CENTROIDS_FILE)

        index = 1
        while True:
            print "Iteration #%i" % index
            updaterJob = KMeansUpdateLocation(args=args + ['--centroids='+CENTROIDS_FILE])
            with updaterJob.make_runner() as updaterJobRunner:
                updaterJobRunner.run()

                newCentroids = getCentroids(updaterJob, updaterJobRunner)
                writeCentroidsToDisk(newCentroids, CENTROIDS_FILE)
                
#                plt.figure(figsize=(8,6)))

                #Check if the kmeans algorithm has converged.
                diff = findBiggestDifference(centroids, newCentroids)
                if diff > 10.0:
                    centroids = newCentroids
                else:
                    #kmeans has converged
                    break

                index +=1
