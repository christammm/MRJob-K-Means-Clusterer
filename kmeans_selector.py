# CMSC 12300 - Computer Science with Applications 3
# Borja Sotomayor, 2013
#

import numpy as np
from mrjob.job import MRJob


class KMeansCentroidSelector(MRJob):

    def __init__(self, args):
        MRJob.__init__(self, args)
        
    def configure_options(self):
        super(KMeansCentroidSelector, self).configure_options()
        
    def getCoordinates(self,_, line):
        (numL,numM) = line.split(',')
        print "Received Coordinates (" , numL, ",", numM , ")"
        g = [int(numL), int(numM)]
        
        yield None, [int(numL), int(numM)]   

    def findRanges(self, _, coords):
        print "finding Ranges 1"
        maxCoord = [0,0]
        minCoord = [1000,1000]
        for coord in coords:
            print coord
            maxCoord = np.maximum(maxCoord, coord)
            print "maxCoord: ", maxCoord
            minCoord = np.minimum(minCoord,coord)
            print "minCoord: " , minCoord
        
        print "Done!!!"
        yield None, maxCoord.tolist()
        yield None, minCoord.tolist()
        
    def chooseCentroids(self, _, maxmin):
        maxCoord = minCoord = np.array(maxmin.next(), dtype=float)
        
        for coord in maxmin:
            maxCoord = np.maximum(maxCoord, coord)
            minCoord = np.minimum(minCoord,coord)
            
        #Set number of clusters you wish to set into k
        k = 3
        average = (maxCoord - minCoord) / k
        
        for times in range(k):
            yield None, (minCoord + average*times).tolist()
            
    def steps(self):
        return [self.mr(mapper=self.getCoordinates,
                        combiner=self.findRanges,
                        reducer=self.chooseCentroids)]
    
if __name__ == '__main__':
    KMeansCentroidSelector.run()