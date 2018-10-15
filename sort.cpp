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
	size_t const size = 100000000;

	MapReducer<VecView, int, VecView, int, VecView> sorter(
			inputReader,
			mapper,
			reducer,
			outputer);

	vector<int> vec1(size);
	vector<int> vec2(size);

	srand(0xdeadbeef);
	for (size_t i = 0; i < size; i++) {
		int num = rand();
		vec1[i] = vec2[i] = num;
	}

	cout << "sorting " << size << " integers" << endl;

	auto mapReduceStart = chrono::high_resolution_clock::now();
	sorter.mapReduce(VecView(vec1.begin(), vec1.end()));
	auto mapReduceFinish = chrono::high_resolution_clock::now();

	cout << "MapReduce time:\t"
		<< chrono::duration_cast<chrono::milliseconds>(mapReduceFinish - mapReduceStart).count()
		<< "ms"
		<< endl;

	auto stlSortStart = chrono::high_resolution_clock::now();
	sort(vec2.begin(), vec2.end());
	auto stlSortFinish = chrono::high_resolution_clock::now();

	cout << "std::sort time:\t"
		<< chrono::duration_cast<chrono::milliseconds>(stlSortFinish - stlSortStart).count()
		<< "ms"
		<< endl;

	cout << "ouputs match ? " << (vec1 == vec2) << endl;
}
