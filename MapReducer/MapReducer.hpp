#pragma once

#include <iostream>
#include <functional>
#include <vector>
#include <utility>
#include <map>
#include <thread>

using namespace std;

const int k_numThreads = 4;
void distribute(function<void(int)> f)
{
	vector<thread> threads;
	for (int threadNum = 0; threadNum < k_numThreads; threadNum++) {
		threads.emplace_back([=] { f(threadNum); });
	}
	for (auto &thread : threads) {
		thread.join();
	}
}

template <class Input, class Key1, class Val1, class Key2, class Val2, class Val3>
class MapReducer
{
	using InputReaderFunc = function<vector<pair<Key1, Val1>>(Input)>;
	using MapFunc = function<vector<pair<Key2, Val2>>(Key1, Val1)>;
	using ReduceFunc = function<pair<Key2, Val3>(Key2, vector<Val2>)>;
	using OutputFunc = function<void(vector<pair<Key2, Val3>>)>;
	public:
		MapReducer(
				InputReaderFunc inputReader,
				MapFunc mapper,
				ReduceFunc reducer,
				OutputFunc outputer) :
			inputReader(inputReader),
			mapper(mapper),
			reducer(reducer),
			outputer(outputer)
		{}

		void mapReduce(Input input)
		{
			vector<pair<Key1, Val1>> inputs = inputReader(move(input));

			// Map stage
			vector<pair<Key2, Val2>> mapOutputs[inputs.size()]; // array of vectors
			distribute([&](int offset) {
				for (size_t i = offset; i < inputs.size(); i += k_numThreads) {
					mapOutputs[i] = mapper(inputs[i].first, move(inputs[i].second));
				}
			});

			// Shuffle stage
			map<Key2, vector<Val2>> mapOutputGroups;
			for (auto &mapOutput : mapOutputs) {
				for (auto &k2v2 : mapOutput) {
					mapOutputGroups[k2v2.first].push_back(k2v2.second);
				}
			}

			// Reduce stage
			vector<pair<Key2, Val3>> outputs(mapOutputGroups.size());
			distribute([&](int offset) {
				auto mapOutputGroup = mapOutputGroups.begin();
				advance(mapOutputGroup, offset);
				for (size_t i = offset; i < mapOutputGroups.size(); i += k_numThreads) {
					outputs[i] = reducer(mapOutputGroup->first, move(mapOutputGroup->second));
					advance(mapOutputGroup, k_numThreads);
				}
			});
			outputer(move(outputs));
		}

	private:
		InputReaderFunc const inputReader;
		MapFunc const mapper;
		ReduceFunc const reducer;
		OutputFunc const outputer;
};
