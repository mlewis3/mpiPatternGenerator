#ifndef PATH_STRUCTURE_H
#define PATH_STRUCTURE_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <list>

using namespace std;
// using boost::property_tree::ptree;

class PathStructure;

class LinkedPairs {
  public:
  int sourceId;
  int targetId;
  int sourceType;  // 0 subj, 1 obj
  int targetType;
};

class ColumnData {
  public:
    vector<string> subjectCol;
    vector<string> objectCol;
    string subjTypeName;
    string objTypeName;
    string predicateName;
    int tripleId;
};

class DataInstance {
   public:
   DataInstance() { numColumns = 0; }
   bool initialize(int patternId, string datasetName, int mode, bool genMode);
   bool loadPatternFile(ColumnData *colData,int patternId, string datasetName, int mode, bool genMode);
   void join(int patternId, string &keyName, string & dirName);

   // Alternating subject and referent type names
   // Ex: A data instance of [123] represents the data joined along path 1-2-3
   vector<ColumnData*>  columnList;
   vector<string> predicateList;
   int numColumns;
   int tripleId;
   vector<string> *tmpSubjList;
   vector<string> *tmpObjList;

};

class StringComparator {
   public:
      bool operator() (const string & a, const string & b) {
         return a < b;
      }
};


class WorkerNode;
class PathObject;

class PatternObject {
   public:
  
   PatternObject() { pathObjectList = new vector<PathObject*>(); }
   PatternObject(vector<PathObject*> *pList) { pathObjectList = pList; } 
   string subj;
   string pred;
   string obj;
   vector<PathObject *> * pathObjectList;   
   
};

class PathObject {
    public:
        
        PathObject(int patternId, string datasetName, int mode, bool genMode, map<int,PatternObject*> *obj);
        bool checkDuplicateConnections(LinkedPairs *linkedPairs);
        bool loadPatternFile(ColumnData *colData, string keyName, string dirName, int mode, bool genMode);
        void join(PathObject *);
        bool makeSiblingLinks(PathObject *targetObject);
        void savePathData(int machineId);
        void addPathContent(PathObject *po, int sourceId, int targetId, int type);
       
        // bool findDuplicates(vector<LinkedPairs *> *pairList);
        // bool containsTriple(int patternId, int pathIndex);
        void connectAppendPropagate(PathStructure *pathStructure, int targetId, string subjName, string objName, string datasetName);
        void findMatch(int sourceId, string tSubjName, string tObjName, bool &hasSubjMatch,
                       bool &hasSubjObjMatch, bool & hasObjSubjMatch);
        void createLinkedPair(LinkedPairs *linkedPair, int sourceId, int targetId, int type);
        void merge(vector<string> &s, vector<string> &t, vector<int> & i, vector<int> &j);
        // PathObject(unsigned long id);
        // void insertPaths(PathStructure *pathstructure);
        // void saveSummaryFile(int rootId, int depth, int workerId);
        // void join(DataInstance *instance,int sourceIndex, int targetIndex, string fileName,  map<string,DataInstance*> *dataMap); // 0 = source -> dest  1 = source <- dest
    vector<LinkedPairs *> *pathContents;
    DataInstance *dataInstance;
    string subj;
    string pred;
    string obj;
    bool updated;
    int patternId;
    bool workerId;
    int objIndex;
    bool generationMode;
    bool containsData;
    map<int,PatternObject*> *objectMap;
	  
};



class PathStructure {
     public:
        PathStructure( vector<string> &list,string datasetFile, int tripleIndex,  bool generationMode); 
        void makeConnection(int sourceId, int targetId, string datasetName, int workerId);
        void updateSubgraph( PathObject *targetObject);
        void buildSubgraph(int pId,int id, string dirName, int workerId);
        void storeTriples(string queryFile, int workerIde);
     // int maxRadius;
     bool generationMode;
     map<int,PatternObject *>  objectMap;
     // vector<vector<PathObject*> *> objectMap;
     vector<int> fringeList;
     vector<int> siblingList;
     vector<string> *tripleList;
     int objIndex;
     int rootId;
};
// PathStructure * PathStructure::structure_Instance = 0;
// class InputTriple;

class WorkerNode {
    public: 
       WorkerNode();
       void setDepth(int index) {
          maxRadius = index;
       }

    string tripleDirectoryName;
    int matchTypes(DataInstance *source, string name);
    void loadTriples(string inputFileName, string datasetFile, int workerId, int numWorkers);
    int insertPaths(PathStructure *structure, DataInstance *source, string key, string typeName);
    void buildSubgraph(int maxDepth, string dirName);
    bool extractFile(string & subjId, string & refId, string & pred, string fileName);
    PathStructure *pathStructure;
    void inputGraphMatch(string input);

    // Query generating functions
    void generateQuery(string queryDir, string patterListingFile, string tripleDataset, int workerId, int numWorkers, int rounds);

    // void storeTriples(string queryId, int workerId);

    // Combine query files
    void combineQueryFiles(string queryDir, string queryFile, int workerId);

    // Bidding
    int * makeBid(int * sendBuffer, string queryFile, int numQueries);
	
	int compareList(vector<LinkedPairs*> &queryList);

    int maxRadius;
    bool generationMode;
    // vector<ExtractedTriple *> rootList;
    // vector<ExtractedTriple *> tripleList;
    vector<string> tripleList;
    // vector<int> mapNodeIndex;
    vector<int> rootList;
    int workerId;
    string queryDirectory;
    // root id is the key
    vector<PathStructure *> pathStructures;
	string datasetFile;
	int * answers;
};


//WorkerList * WorkerList::workerListInstance = 0;
#endif
