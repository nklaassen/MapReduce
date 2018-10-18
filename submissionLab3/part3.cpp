#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <utility>

using namespace std;

vector<string> tokenizeData(ifstream &file) {
	vector<string> wordVector;
	string word;

	while (file >> word) {
		wordVector.push_back(word);
	}
	return wordVector;
}
vector<pair<string, int> > tallyWords(vector<string> &wordVector) {
	int instances = 1; // tracking instances of a word
	int vectorSize = wordVector.size();
	string word = wordVector[0];
	pair<string, int> countedWord;
	vector<pair<string, int>> countedWordVector;

	for (int i = 1; i < vectorSize; i++) {
		if (word != wordVector[i]) {
			countedWord.first = word;
			countedWord.second = instances;
			countedWordVector.push_back(countedWord);
			instances = 0;
			word = wordVector[i];
		}
		instances++;
	}
	countedWord.first = word;
	countedWord.second = instances;
	countedWordVector.push_back(countedWord);
	return countedWordVector;
}
bool sortByCount(const pair<string, int> &a,
	const pair<string, int> &b)
{
	return (a.second > b.second);
}
void printCount(vector<pair<string, int> > &talliedVector) {
	sort(talliedVector.begin(),talliedVector.end(),sortByCount);
	for (int i = 0; i < talliedVector.size(); i++)
	{
		cout << talliedVector[i].first << ":\t" << talliedVector[i].second << endl;
	}
}

int main(int argc, char **argv)
{
	vector<pair<string, int> > countedWords;

	// open file (from Nic's code)
	if (argc < 2) {
		cerr << "format: " << argv[0] << " [filename]" << endl;
		return 1;
	}
	ifstream datafile(argv[1]);
	if (datafile.fail()) {
		cerr << "failed to open file" << endl;
		return 2;
	}
	
	// process file
	vector<string> words = tokenizeData(datafile); // tokenize file
	sort(words.begin(), words.end()); // sort words in alphabetical order
	countedWords = tallyWords(words); // create word-count pairs
	printCount(countedWords); // order by word count and print
}
