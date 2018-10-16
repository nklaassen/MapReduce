#include "MapReducer/MapReducer.hpp"

#include <iostream>
#include <functional>
#include <vector>
#include <utility>
#include <fstream>
#include <algorithm>
#include <string>

using namespace std;

vector<pair<int, string>> inputReader(ifstream input)
{
	auto vec = vector<pair<int, string>>();
	string word;
	for (int i = 0; input >> word; i++) {
		vec.emplace_back(i, word);
	}
	return vec;
}
vector<pair<string, int>> mapper(int wordNum, string word)
{
	(void) wordNum;
	return { make_pair(word, 1) };
}
pair<string, int> reducer(string word, vector<int> counts)
{
	return make_pair(word, counts.size());
}
void outputer(vector<pair<string, int>> wordCounts)
{
	std::sort(wordCounts.rbegin(), wordCounts.rend(),
			[](const pair<string, int> &a, const pair<string, int> &b) {
				return a.second < b.second;
			});

	for (auto wordCount : wordCounts) {
		cout << wordCount.first << ":\t" << wordCount.second << endl;
	}
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		cerr << "format: " << argv[0] << " filename" << endl;
		return 1;
	}

	ifstream input(argv[1]);
	if (input.fail()) {
		cerr << "bad input" << endl;
		return 2;
	}

	MapReducer<ifstream, int, string, string, int, int> wordCounter(inputReader, mapper, reducer, outputer);

	wordCounter.mapReduce(move(input));
}
