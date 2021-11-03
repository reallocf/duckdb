"""
z is an integer that follows a zipfian distribution
and v is a double that follows a uniform distribution
in [0, 100]. θ controls the zipfian skew, n is the table
size, and g specifies the number of distinct z values
"""
import random 
import sys
import pandas as pd
import numpy as np
import argparse

class ZipfanGenerator(object):
    def __init__(self, n_groups, zipf_constant, card):
        self.n_groups = n_groups
        self.zipf_constant = zipf_constant
        self.card = card
        self.initZipfan()

    def zeta(self, n, theta):
        ssum = 0.0
        for i in range(0, n):
            ssum += 1. / pow(i+1, theta)
        return ssum

    def initZipfan(self):
        zetan = 1./self.zeta(self.n_groups, self.zipf_constant)
        proba = [.0] * self.n_groups
        proba[0] = zetan
        for i in range(0, self.n_groups):
            proba[i] = proba[i-1] + zetan / pow(i+1., self.zipf_constant)
        self.proba = proba

    def nextVal(self):
        uni = random.uniform(0.1, 1.01)
        lower = 0
        for i, v in enumerate(self.proba):
            if v >= uni:
                break
            lower = i
        return lower

    def getAll(self):
        result = []
        for i in range(0, self.card):
            result.append(self.nextVal())
        return result

parser = argparse.ArgumentParser()
parser.add_argument('groups', type=int, help='Number of unique groups')
parser.add_argument('card', type=int, help='cardinality')
parser.add_argument('a', type=float, help='zipfan param')
args = parser.parse_args()

groups = args.groups
card = args.card
a = args.a

z = ZipfanGenerator(groups, a, card)
zipfan = z.getAll()
vals = np.random.uniform(0, 100, card)
idx = list(range(0, card))
df = pd.DataFrame({'idx':idx, 'z': zipfan, 'v': vals})
df.to_csv("zipfan_g"+str(groups)+"_card"+str(card)+"_a"+str(a)+".csv", index=False)
