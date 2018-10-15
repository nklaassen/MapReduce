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

using IntVec = vector<int>;
using Int_IntVec = pair<int, IntVec>;

vector<Int_IntVec> inputReader(IntVec const& input)
{
	auto const numChunks = 4;
	auto chunkSize = input.size() / numChunks + 1;
	vector<pair<int,IntVec>> vec;
	for (auto i = 0; i < numChunks - 1; i++) {
		vec.emplace_back(i, IntVec(input.begin() + i * chunkSize, input.begin() + (i+1) * chunkSize));
	}
	vec.emplace_back(numChunks - 1, IntVec(input.begin() + (numChunks - 1) * chunkSize, input.end()));
	return vec;
}
vector<Int_IntVec> mapper(int chunkId, IntVec chunk)
{
	sort(chunk.begin(), chunk.end());
	return { make_pair(chunkId / 2, move(chunk)) };
}
Int_IntVec reducer(int chunksId, vector<IntVec> sortedChunks)
{
	IntVec output;
	merge(
		sortedChunks[0].begin(), sortedChunks[0].end(),
		sortedChunks[1].begin(), sortedChunks[1].end(),
		back_inserter(output));
	return make_pair(chunksId, move(output));
}
void outputer(IntVec& output, vector<Int_IntVec> sortedChunks)
{
	merge(
		sortedChunks[0].second.begin(), sortedChunks[0].second.end(),
		sortedChunks[1].second.begin(), sortedChunks[1].second.end(),
		back_inserter(output));
}

int main()
{
	size_t const size = 100000000;

	cout << "sorting " << size << " integers" << endl;

	IntVec input(size);
	IntVec output;
	output.reserve(size);

	MapReducer<IntVec, int, IntVec, int, IntVec> sorter(
			inputReader,
			mapper,
			reducer,
			[&] (vector<Int_IntVec> a) { outputer(output, a); });


	srand(0xdeadbeef);
	for_each(input.begin(), input.end(), [](int &i) { i = rand(); });

	auto mapReduceStart = chrono::high_resolution_clock::now();
	sorter.mapReduce(move(input));
	auto mapReduceFinish = chrono::high_resolution_clock::now();

	cout << "MapReduce time:\t"
		<< chrono::duration_cast<chrono::milliseconds>(mapReduceFinish - mapReduceStart).count()
		<< "ms"
		<< endl;

	IntVec vec(size);
	srand(0xdeadbeef);
	for_each(vec.begin(), vec.end(), [](int &i) { i = rand(); });

	auto stlSortStart = chrono::high_resolution_clock::now();
	sort(vec.begin(), vec.end());
	auto stlSortFinish = chrono::high_resolution_clock::now();

	cout << "std::sort time:\t"
		<< chrono::duration_cast<chrono::milliseconds>(stlSortFinish - stlSortStart).count()
		<< "ms"
		<< endl;

	cout << "ouputs match ? " << (vec == output) << endl;
}
