#ifndef RDF_READER_H
#define RDF_READER_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <sstream>

using namespace std;
// using boost::property_tree::ptree;


class TypeComparator {
   public:
     bool operator() (const string &a, const string & b) {
         return a < b;
     }
};


class PatternMap {
      public:
         PatternMap() { };

         PatternMap(const PatternMap &a) {
            instance = a.instance;
            pattern = a. pattern;
         }
  
      string instance;
      string pattern;
      unsigned long patternIndex;
};


class PatternComparator {
    public:
      bool operator() (PatternMap * const &a, PatternMap * const & b ) {
          return a->instance < b->instance;
      }
};

class GraphStruct {
   public:
      GraphStruct(string line, int index){triplePattern = line; id = index;};
   
   int id; 
   string triplePattern;
   vector<GraphStruct*> outboundList;
};

class ReaderRDF {
public:
	ReaderRDF();
	void normalizeTypeFile(string inputTypeFile, int numLines);
	void normalizeTripleFile(string inputTripleFile, int numLines);
	
	// Create instance - type files for each worker
	void createTypes(string inputDir, string outputDir, int workerId, int numWorkers, int numLines);
	
	// Collect each worker instance - type files into one dataset
	void combineTypeFiles(string outputDir, string outFileName, int numWorkers);
	
	// Create type triple files  (332)
	void createTypeEntries(string inputTripleFile, string inputTypeFile, string outputDir, int workerId, int numWorkers, int numLines);
	
	void loadMapFiles(string inputTripleFile, int size);
	
	void createPatternFiles();
	
	// Assemble triple files for each worker (561)
	void assemblePatternFiles(string input, string inputTripleDataset, string inputOffset, int numWorkers);
	
	// Save type to triple mappings within a file
	void storeTriplesToFile(map<string,vector<string> *> &map, string datasetFile, string offsetFile);
	
	// Return the number of lines in a file
	int readFileSize(ifstream & inputFileStream, bool top);
	
	// Help function
	void appendLine(char *buffer, string &pattern, int offset, int &counter);
	
	
	// Query functions --------------------------
	void cacheTriples(string fileName);
	
	void selectQueries(string queryFile, int numQueries, int *buffer);
	
	
	static ReaderRDF * instance() {
		if (ReaderRDF::readerInstance == 0) {
			ReaderRDF::readerInstance = new ReaderRDF();
		} 
		return ReaderRDF::readerInstance;
	}
    
    vector<string> subjectList;
    vector<string> tripleIndexList;
    vector<string> objectList;
    vector<string> predicateList;
    map<string,vector<string> *> tmpMap;
    set<string,TypeComparator> fileSet;
	int worker_Id;
	int num_Workers;
	string output_Dir;
	//    string datasetName;
	//    string instanceReferent;
	//    string predicate;
	//    string instanceSubject;
	//    bool foundSubject;
	//    bool foundPredicate;
	//    bool foundReferent;
	//    long subjectId;
	//    long referentId;
	
    static ReaderRDF *readerInstance;
};


#endif
