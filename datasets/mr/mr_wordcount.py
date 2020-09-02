from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordCounter(MRJob):
    def mapper_split_on_word(self, _, line):
        for word in line.split():
            yield word, 1

    def reducer_count_word(self, word, occurrences):
        yield word, sum(occurrences)

    def steps(self):
        return [
            MRStep(mapper = self.mapper_split_on_word,
            reducer = self.reducer_count_word)
    ]

if __name__ == '__main__':
    MRWordCounter.run()
