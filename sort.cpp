#include "MapReducer.h"

#include <iostream>
#include <functional>
#include <vector>
#include <utility>
#include <fstream>
#include <algorithm>
#include <string>
#include <cstdlib>
#include <iterator>
#include <chrono>
#include <thread>

using namespace std;

struct VecView
{
	vector<int>::iterator begin;
	vector<int>::iterator end;
	size_t size() { return distance(begin, end); }
	VecView(vector<int>::iterator begin, vector<int>::iterator end)
		: begin(begin)
		, end(end)
	{}
	VecView()
	{}
};

vector<pair<int, VecView>> inputReader(VecView input)
{
	auto const numChunks = 4;
	auto chunkSize = input.size() / numChunks + 1;
	vector<pair<int, VecView>> vec;
	for (auto i = 0; i < numChunks - 1; i++) {
		vec.emplace_back(i, VecView(input.begin + i * chunkSize, input.begin + (i+1) * chunkSize));
	}
	vec.emplace_back(numChunks - 1, VecView(input.begin + (numChunks - 1) * chunkSize, input.end));
	return vec;
}
vector<pair<int, VecView>> mapper(int chunkId, VecView chunk)
{
	sort(chunk.begin, chunk.end);
	return { make_pair(chunkId / 2, chunk) };
}
pair<int, VecView> reducer(int chunksId, vector<VecView> sortedChunks)
{
	VecView a = sortedChunks[0], b = sortedChunks[1];
	if (distance(a.begin, b.begin) < 0)
		swap(a, b);
	inplace_merge(a.begin, a.end, b.end);
	return make_pair(chunksId, VecView(a.begin, b.end));
}
void outputer(vector<pair<int, VecView>> sortedChunks)
{
	VecView a = sortedChunks[0].second, b = sortedChunks[1].second;
	if (distance(a.begin, b.begin) < 0)
		swap(a, b);
	inplace_merge(a.begin, a.end, b.end);
}

int main()
{
	MapReducer<VecView, int, VecView, int, VecView> sorter(
			inputReader,
			mapper,
			reducer,
			outputer);

	srand(0xdeadbeef);

	// output timing data in csv format
	cout << "array size, MapReduce, std::sort" << endl;

	for (int size = 100000; size <= 400000; size *= 2) {
		vector<int> vec1(size);
		vector<int> vec2(size);
		for (int i = 0; i < 100; i++) {
			// initialize arrays to random data
			for (int i = 0; i < size; i++) {
				vec1[i] = vec2[i] = rand();
			}

			// time mapReduce sort
			auto mapReduceStart = chrono::high_resolution_clock::now();
			sorter.mapReduce(VecView(vec1.begin(), vec1.end()));
			auto mapReduceFinish = chrono::high_resolution_clock::now();

			// time std::sort
			auto stlSortStart = chrono::high_resolution_clock::now();
			std::sort(vec2.begin(), vec2.end());
			auto stlSortFinish = chrono::high_resolution_clock::now();

			cout << size
				<< "," << chrono::duration_cast<chrono::microseconds>(mapReduceFinish - mapReduceStart).count()
				<< "," << chrono::duration_cast<chrono::microseconds>(stlSortFinish - stlSortStart).count()
				<< endl;
		}
	}
}
