import numpy as np
import pickle as pk

from mrjob.job import MRJob

class KMeansUpdateLocation(MRJob):
    
    def configure_options(self):
        super(KMeansUpdateLocation, self).configure_options()
        self.add_file_option('--centroids')
        
    def get_centroids(self):
        centroidsFile = open(self.options.centroids)
        centroids = pk.load(centroidsFile)
        centroidsFile.close()
        return centroids
    
    #Mapper
    def assignClusters(self, _, line):
        l = line.split()
        if len(l) == 1:
            return

        coord = np.array([float(x) for x in l[:-1]])
        centroids = self.get_centroids()
        
        distances = [np.linalg.norm(coord - c) for c in centroids]
        cluster = np.argmin(distances)

        yield int(cluster), coord.tolist()
        
    def partialSum(self, cluster, coords):
        listCoords = np.array(coords.next())
        count = 1
        for coord in listCoords:
            listCoords = listCoords + coord
            count = count + 1
        
        yield cluster, (listCoords.tolist(),count)
        
    def computeAverage(self, cluster, listOfPartialSums):
        sum, count = listOfPartialSums.next()
        sum = np.array(sum)
        for coords, index in listOfPartialSums:
            sum += coords
            count += index
        
        yield cluster, (sum / count).tolist() 
        
    def steps(self):
        return [self.mr(mapper=self.assignClusters,
                        combiner=self.partialSum,
                        reducer=self.computeAverage)]

if __name__ == '__main__':
    KMeansUpdateLocation.run()
                        
                