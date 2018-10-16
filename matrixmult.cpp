#include "MapReducer/MapReducer.hpp"

#include <iostream>
#include <functional>
#include <vector>
#include <tuple>
#include <utility>
#include <fstream>
#include <algorithm>
#include <string>
#include <iomanip>

#define MAX_VALUE 10.0
#define PRINT_STOP 8

using namespace std;

int msize;
clock_t start_time;

// Generate random matrices. Key is (matrix, row, col)
//          key1                  val1
vector<pair<tuple<int, int, int>, double>> inputReader(int size)
{
	msize = size;
	auto vec = vector<pair<tuple<int, int, int>, double>>();
	for (int v = 0; v < 2; v++) {
		for (int i = 0; i < size; i++) {
			for (int j = 0; j < size; j++) {
				double val = ((double)rand()/RAND_MAX) * MAX_VALUE;
				if(i < PRINT_STOP && j < PRINT_STOP) cout << setw(7) << setprecision(4) << val;
				else if(i < PRINT_STOP && j == PRINT_STOP) cout << "...";
				vec.emplace_back(make_tuple(v, i, j), val);
			}
			if(i < PRINT_STOP) cout << endl;
			else if(i == PRINT_STOP) cout << "...";
		}
		cout << "--------" << endl;
	}
	start_time = clock();
	return vec;
}

//           key2           val2                                 key 1                    val 1
vector<pair<pair<int, int>, tuple<int, int, double>>> mapper(tuple<int, int, int> maddr, double val)
{
	vector<pair<pair<int, int>, tuple<int, int, double>>> vec;

	int matrix = get<0>(maddr);
	if(matrix == 0) {
		auto rval = make_tuple(matrix, get<2>(maddr), val);
		for(int i = 0; i < msize; i++) {
			auto key = make_pair(get<1>(maddr), i);
			vec.emplace_back(key, rval);
		}
	} else {
		auto rval = make_tuple(matrix, get<1>(maddr), val);
		for(int i = 0; i < msize; i++) {
			auto key = make_pair(i, get<2>(maddr));
			vec.emplace_back(key, rval);
		}
	}

	return vec;
}

//   key2            val3
pair<pair<int, int>, double> 
//      key2            vector<val2>
reducer(pair<int, int> key, vector<tuple<int, int, double>> values)
{
	double output = 0;
	vector<pair<int, double>> list[2];
	for(auto it = values.begin(); it != values.end(); it++) {
		list[get<0>(*it)].push_back(make_pair(get<1>(*it), get<2>(*it)));
	}
	if(list[0].size() != list[1].size()) {
		cerr << "Invalid matrix dimensions" << endl;
		exit(1);
	}
	for(int i = 0; i < 2; i++) {
		sort(list[i].begin(), list[i].end(), 
			[](const pair<int, double> &a, const pair<int, double> &b) -> bool {
				return a.first > b.first;
			});
	}
	for(size_t i = 0; i < list[0].size(); i++) {
		output += list[0][i].second * list[1][i].second;
	}
	return make_pair(key, output);
}

void outputer(vector<pair<pair<int, int>, double>> products)
{
        
        float elapsed = float(clock() - start_time) / CLOCKS_PER_SEC;
	double matrix[msize][msize];
	for(auto it = products.begin(); it != products.end(); it++)
	{
		matrix[it->first.first][it->first.second] = it->second;
	}
	for (int i = 0; i < msize; i++) {
		for (int j = 0; j < msize; j++) {
			if(i < PRINT_STOP && j < PRINT_STOP) cout << setw(9) << setprecision(2) << matrix[i][j];
			else if(i < PRINT_STOP && j == PRINT_STOP) cout << "...";
		}
		if(i < PRINT_STOP) cout << endl;
		else if(i == PRINT_STOP) cout << "...";
	}
	cout << setprecision(6) << "Elapsed time: " << elapsed << endl;
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		cerr << "format: " << argv[0] << " MATRIX_SIZE" << endl;
		return 1;
	}

        int msize = atoi(argv[1]);

        //        input, key1, val1, key2, val2
	MapReducer<int, tuple<int, int, int>, double, pair<int, int>, tuple<int, int, double>, double> 
		wordCounter(inputReader, mapper, reducer, outputer);

	wordCounter.mapReduce(msize);
}
