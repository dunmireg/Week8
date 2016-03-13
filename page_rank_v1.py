from __future__ import division
 
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol
 
from collections import defaultdict
from operator import itemgetter
import os 
 

# Global variables 
numberOfNodes = 0 


class pageRank(MRJob):
 
     """ This class implements the page-rank calculation. """


     def configure_options(self):

          """ Load options for the class. """

          super(pageRank, self).configure_options()

          self.add_passthrough_option('--alpha',
               default=0.85, help='alpha: Dampening factor for teleportation in PageRank')

          self.add_passthrough_option('--iterations',
               default=10, help='iterations: number of iterations for PageRank')


     def load_options(self, args):

          """ Initializes the arguments for each class. """

          super(pageRank, self).load_options(args)

          self.alpha = self.options.alpha
          self.iterations = self.options.iterations


     def mapper_init_pr(self, _, line):

          """ This initializes the PageRank algorithm by assembling the node list 
          for the initial PageRank values. """

          # Initiate 
          global numberOfNodes 

          # Parse 
          line = line.split('\t')
          node = line[0]
          adjacencyList = eval(line[1])

          # Track 
          for neighbor in adjacencyList.keys(): 

               # Emit raw nodes
               yield neighbor, None

          numberOfNodes += 1

          # Pass values
          yield node, adjacencyList


     def reducer_init_pr(self, node, initTuple):

          """ This attaches initial PageRanks for the algorithm. """

          adjacencyList = dict()

          # Re-discover 
          for element in initTuple:
               if isinstance(element, dict):
                    adjacencyList = element 

          # Initialize PR
          PageRank = float(1) / float(numberOfNodes)

          # Emit
          yield node, (adjacencyList, PageRank)


     def mapper_iterate_pr(self, node, nodeTuple):

          """ This projects all of the PageRank weights for each node's neighbor. """

          adjacencyList, PageRank = nodeTuple

          if not adjacencyList:
               pass

          else: 

               # Emit PR 
               for neighbor in adjacencyList.keys(): 
                    yield neighbor, PageRank / len(adjacencyList)

          # Emit structure 
          yield node, adjacencyList


     def reducer_iterate_pr(self, node, PRNodeObject):

          """ This reconstructs the graph structure form the updated PageRanks. """

          updatedPR = 0

          # Combine PR 
          for value in PRNodeObject:
               if isinstance(value, dict):
                    adjacencyList = value 

               else: 
                    updatedPR += value 

          # Damping factor 
          updatedPR = ((1 - self.alpha) / numberOfNodes) + self.alpha * updatedPR

          # Emit 
          yield node, (adjacencyList, updatedPR)


     def mapper_sort(self, node, nodeTuple):

          """ Emits the page rank for each node. """

          adjacencyList, PageRank = nodeTuple

          yield None, (node, PageRank)


     def reducer_sort(self, _, PageRankPair):

          """ Keeps the top 100 PageRank values. """

          sortedList = []

          # Iterate and remove 
          for node, score in PageRankPair:

               sortedList.append((node, score))
               sortedList = sorted(sortedList, key=itemgetter(1), reverse=True)

               if len(sortedList) > 100: 
                    sortedList.pop()


          # Emit 
          for node, score in sortedList: 
               yield node, score


     def steps(self):

          """ Determines the steps for the job. Has two phases- initiate PR and iterate. """

          initializeStep = [

               MRStep(mapper=self.mapper_init_pr, 
                         reducer=self.reducer_init_pr)

          ]

          iterateStep = [

               MRStep(mapper=self.mapper_iterate_pr, 
                         reducer=self.reducer_iterate_pr)         

          ]

          sortStep = [

               MRStep(mapper=self.mapper_sort, 
                         reducer=self.reducer_sort)

          ]

          return initializeStep + iterateStep * 10 + sortStep
 
 
if __name__ == '__main__':
               pageRank().run()                             