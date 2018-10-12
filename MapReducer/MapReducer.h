#pragma once

#include <functional>
#include <vector>
#include <utility>
#include <map>

template <class Input, class Key, class Val>
class MapReducer
{
	public:
		MapReducer(
				std::function<std::vector<Key>(Input)> inputReader,
				std::function<std::pair<Key, Val>(Key)> mapper,
				std::function<std::pair<Key, Val>(std::vector<std::pair<Key, Val>>)> reducer,
				std::function<void(std::vector<std::pair<Key, Val>>)> outputer) :
			inputReader(inputReader),
			mapper(mapper),
			reducer(reducer),
			outputer(outputer)
		{}

		void mapReduce(Input input)
		{
			auto keys = inputReader(input);
			std::map<Key, std::vector<std::pair<Key, Val>>> keyValsMap;
			for (auto key : keys) {
				keyValsMap[key].push_back(mapper(key));
			}
			std::vector<std::pair<Key, Val>> keyVals;
			for (auto kvs : keyValsMap) {
				keyVals.push_back(reducer(kvs.second));
			}
			outputer(keyVals);
		}

	private:
		std::function<std::vector<Key>(Input)> const inputReader;
		std::function<std::pair<Key, Val>(Key)> const mapper;
		std::function<std::pair<Key, Val>(std::vector<std::pair<Key, Val>>)> const reducer;
		std::function<void(std::vector<std::pair<Key, Val>>)> const outputer;
};
