#pragma once

#include <iostream>
#include <functional>
#include <vector>
#include <utility>
#include <map>
#include <thread>
#include <omp.h>

using namespace std;

template <
	class Input,
	class ReaderOutputKey,
	class ReaderOutputVal,
	class MapOutputKey,
	class MapOutputVal,
	class ReduceOutputKey,
	class ReduceOutputVal
>
class MapReducer
{
	using MapInputKey = ReaderOutputKey;
	using MapInputVal = ReaderOutputVal;

	using ReduceInputKey = MapOutputKey;
	using ReduceInputVal = MapOutputVal;

	using OutputInputKey = ReduceOutputKey;
	using OutputInputVal = ReduceOutputVal;

	using ReaderFunc = vector<pair<ReaderOutputKey, ReaderOutputVal>> (*) (Input);
	using MapFunc = vector<pair<MapOutputKey, MapOutputVal>> (*) (MapInputKey, MapInputVal);
	using ReduceFunc = pair<ReduceOutputKey, ReduceOutputVal> (*) (ReduceInputKey, vector<ReduceInputVal>);
	using OutputFunc = void (*) (vector<pair<OutputInputKey, OutputInputVal>>);

	public:
		MapReducer(
				ReaderFunc inputReader,
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
			vector<pair<ReaderOutputKey, ReaderOutputVal>> inputs = inputReader(move(input));

			// Map stage
			vector<vector<pair<MapOutputKey, MapOutputVal>>> mapOutputs(inputs.size());
#pragma omp parallel for
			for (size_t i = 0; i < inputs.size(); i++) {
				mapOutputs[i] = mapper(inputs[i].first, move(inputs[i].second));
			}

			inputs.resize(0); // don't need this anymore, could be quite large

			// Shuffle stage, group map outputs by key
			map<MapOutputKey, vector<MapOutputVal>> mapOutputGroups;
			for (auto &mapOutput : mapOutputs) {
				for (auto &[key, val] : mapOutput) {
					mapOutputGroups[key].push_back(val);
				}
			}

			mapOutputs.resize(0); // don't need this anymore, could be quite large

			// Reduce stage
			vector<pair<ReduceOutputKey, ReduceOutputVal>> outputs(mapOutputGroups.size());
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
		ReaderFunc const inputReader;
		MapFunc const mapper;
		ReduceFunc const reducer;
		OutputFunc const outputer;
};
