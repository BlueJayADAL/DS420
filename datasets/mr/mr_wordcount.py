from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordCounter(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            yield word, 1

    def reducer(self, word, occurrences):
        yield word, sum(occurrences)

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
            reducer = self.reducer)
    ]

if __name__ == '__main__':
    MRWordCounter.run()
