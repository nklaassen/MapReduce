#pragma once

#include <iostream>
#include <functional>
#include <vector>
#include <utility>
#include <map>
#include <thread>
#include <omp.h>

using namespace std;

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
			// Input stage
			vector<pair<Key1, Val1>> inputs = inputReader(move(input));

			// Map stage
			vector<vector<pair<Key2, Val2>>> mapOutputs(inputs.size()); // vector of vectors
#pragma omp parallel for
			for (size_t i = 0; i < inputs.size(); i++) {
				mapOutputs[i] = mapper(inputs[i].first, move(inputs[i].second));
			}

			inputs.resize(0); // don't need this anymore, could be quite large

			// Shuffle stage, group map outputs by key
			map<Key2, vector<Val2>> mapOutputGroups;
			for (auto &mapOutput : mapOutputs) {
				for (auto &k2v2 : mapOutput) {
					mapOutputGroups[k2v2.first].push_back(k2v2.second);
				}
			}

			mapOutputs.resize(0); // don't need this anymore, could be quite large

			// Reduce stage
			vector<pair<Key2, Val3>> outputs(mapOutputGroups.size());
#pragma omp parallel
			{
				const int offset = omp_get_thread_num();
				const int stride = omp_get_num_threads();
				auto mapOutputGroup = mapOutputGroups.begin();
				advance(mapOutputGroup, offset);
				for (size_t i = offset; i < mapOutputGroups.size(); i += stride) {
					outputs[i] = reducer(mapOutputGroup->first, move(mapOutputGroup->second));
					advance(mapOutputGroup, stride);
				}
			}

			// Output stage
			outputer(move(outputs));
		}

	private:
		InputReaderFunc const inputReader;
		MapFunc const mapper;
		ReduceFunc const reducer;
		OutputFunc const outputer;
};
