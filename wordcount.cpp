#include "MapReducer.h"

#include <iostream>
#include <functional>
#include <vector>
#include <utility>
#include <fstream>
#include <algorithm>

using namespace std;

vector<string> inputReader(ifstream &input)
{
	auto vec = vector<string>();
	string str;
	while (input >> str) {
		vec.push_back(str);
	}
	return vec;
}
pair<string, int> mapper(string key)
{
	return pair<string, int>(key, 1);
}
pair<string, int> reducer(vector<pair<string, int>> keyVals)
{
	return pair<string, int>(keyVals[0].first, keyVals.size());
}
void outputer(vector<pair<string, int>> keyVals)
{
	std::sort(keyVals.rbegin(), keyVals.rend(),
			[](const pair<string, int> &a, const pair<string, int> &b) {
				return a.second < b.second;
			});

	for (auto i : keyVals) {
		cout << i.first << ":\t" << i.second << endl;
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

	MapReducer<ifstream&, string, int> wordCounter(inputReader, mapper, reducer, outputer);

	wordCounter.mapReduce(input);
}
