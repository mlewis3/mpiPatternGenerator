#include <string>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>
#include <stdio.h>
#include <utility>
#include <stdlib.h>
#include <cstring> // memset
// #include <boost/foreach.hpp>
// #include <boost/filesystem/operations.hpp>
// #include <boost/filesystem/path.hpp>
// #include <boost/algorithm/string.hpp>
#include "rdfReader.hxx"
#include "pathStructure.hxx"

using namespace std;
// using namespace boost;
// namespace fs = boost::filesystem;

ReaderRDF * ReaderRDF::readerInstance = 0;
ofstream errorFileStream;
ofstream errorStreamLog;
string getEndWord(string);
string filterString(string);
bool isOpen(string &fileName);
bool contains(string &str1, string &str2);
static void splitString(vector<string> &splitStrings, string &line, string & delim);
// int readFileSize(ifstream & inputFileStream, bool top);

void saveOpenPattern(string & outputFile, string & fileName, string & subj, string & pred, string & obj);
void saveToFile(vector<string> & subjectList, vector<string> & referentList, string outDir, string listFileName, string subj, string pred, string ref);
void findPatterns(vector<string> & patternList, multimap<string,string,TypeComparator> &mapList, string tripleComponent,int index);
string getTriplePattern(string fileName, int index); 
void printList(const vector<GraphStruct *> & nodeList, string outputFile);
bool findMatches(GraphStruct *newNode, vector<GraphStruct *> & inNodes);
void generateOutList(vector<GraphStruct *> & input, vector<GraphStruct *> & out);

ReaderRDF::ReaderRDF() {
}

void ReaderRDF::normalizeTypeFile(string inTypeFile, int countLimit) {
	int typeCount = 0;
	string line;
        // 100 million lines of type files
	
        stringstream typeCountStream;
        stringstream initialCountStream;
        initialCountStream << countLimit;
        int lineSize = 200;
        string outTypeFile = inTypeFile + "-" + initialCountStream.str();
	// ofstream normalizeTypeFileStream;
        // normalizeTypeFileStream.open("normalize.txt");

        int count = 0;
	
	if (isOpen(outTypeFile) == false) {
		ifstream inputTypeStream;
		ofstream outputTypeStream;
		inputTypeStream.open(inTypeFile.c_str());
		outputTypeStream.open(outTypeFile.c_str());
		// normalizeTypeFileStream << " Opening " << outTypeFile << endl;
		// Type file
		while (getline(inputTypeStream,line)) {
                        if (line.size() > lineSize) continue;
                        count++;
			typeCount++;
                        if (countLimit > 0 && count == countLimit) break;
			for (int i = 0; i < lineSize; i++) {
				if (i < line.size()) {
					outputTypeStream << line[i];
				}else {
					if ( i == lineSize -1) outputTypeStream << '\n';
                            		else outputTypeStream << ' ';
				}  
			}
		}

		typeCountStream << typeCount;
		string typeCountString = typeCountStream.str();
		for (int i = 0; i < 20; i++) {
			if (i < typeCountString.size() ) {
				outputTypeStream << typeCountString[i];
			}else {
				if ( i == 19) outputTypeStream << '\n';
                        	else outputTypeStream << ' ';
			}
		}
		// normalizeTypeFileStream << " Exiting normalizeTypeFile " << endl;
		// normalizeTypeFileStream.close();
		inputTypeStream.close();
		outputTypeStream.close();
	}
}

void ReaderRDF::normalizeTripleFile(string inTripleFile, int countLimit){
	stringstream tripleCountStream;
        stringstream initialCountStream;
        initialCountStream << countLimit;       
 
	string outTripleFile = inTripleFile + "-" + initialCountStream.str();

	int mapCount = 0;
	string line;
	ofstream normalizeTripleStream;
        normalizeTripleStream.open("normalizeTriple.txt");

        // 100 million lines of triples
        int count = 0;
	
	// Map file
	if (isOpen(outTripleFile) == false){
		ifstream inputTripleStream;
		ofstream outputTripleStream;
		inputTripleStream.open(inTripleFile.c_str());
		outputTripleStream.open(outTripleFile.c_str());
		normalizeTripleStream << " Opening " << outTripleFile << endl;

		while (getline(inputTripleStream,line)) {
                        if (line.size() > 400) continue;
                        count++;
                        mapCount++;
                        if (countLimit > 0 && count == countLimit) break;
			for (int i = 0; i < 400; i++) {
				if (i < line.size() ) {
					outputTripleStream << line[i];
				}else {
					if (i == 399) outputTripleStream << '\n';
					else outputTripleStream << ' ';
				}
			}
		}
		tripleCountStream << mapCount;
		string tripleCountString = tripleCountStream.str();
		for (int i = 0; i < 20; i++) {
			if (i < tripleCountString.size() ) {
				outputTripleStream << tripleCountString[i];
			} else {
				if (i == 19) outputTripleStream << '\n';
				else outputTripleStream << ' ';
			}
		}
		
	        normalizeTripleStream << " Exiting normalizing triple " << endl;
		normalizeTripleStream.close();
		inputTripleStream.close();
		outputTripleStream.close();
	}
	
}


// Creating URI-type files from an n-triple type dataset 
void ReaderRDF::createTypes(string dataset, string outputDir, int workerId, int numWorkers) {
	ifstream inputFileStream;
	stringstream initialCountStream;
	// int count = 25000000;
	initialCountStream << count;
	stringstream newStream;
	string inputFile = newStream.str();
	int length = 0;  // Total size of the n-tripe type buffer for this node
	int chunkSize;
	int lCount = 0;
	char *buffer;
	multimap<string,string> tmpMap;
	multimap<string,string>::iterator iter;
	multimap<string,string>::iterator iterRange;
	pair< multimap<string,string>::iterator,multimap<string,string>::iterator > patternRange;
	
	
	inputFileStream.open(dataset.c_str());
	
	if (inputFileStream) {

#ifdef DEBUG
             cout << " createTypes-open " << workerId << endl;
#endif

	     if (workerId == 0) eFileStream << " reading file Size " << endl;
	     int lCount = readFileSize(inputFileStream,false);
	     if (workerId == 0) eFileStream << " number of lines " << lCount <<  endl;
	     chunkSize = int( (lCount / numWorkers) + .5);
	     //chunkSize = 10000000;
	     int offset = workerId * chunkSize * 200;
	     if (workerId == numWorkers -1) {
		     length = (lCount - (workerId * chunkSize)) * 200;
	     } else {
		     length = chunkSize * 200;
	     }
	     
	     
	     if (workerId == 0) eFileStream << "Chunksize " << chunkSize << endl;
	     inputFileStream.seekg(offset,inputFileStream.beg);
	     
	     int lineOffset = 200;
	     bool duplicate = false;
	     buffer = new char[length];
	     inputFileStream.read(buffer,length);
	     inputFileStream.close();
	     if (workerId == 0) eFileStream << "Allocated " << length << " workerId " << workerId << endl;
	     for ( int i = 0; i < length; i += lineOffset) {
		     int lineCount = i;
		     int endLineCount = i + lineOffset;
		     stringstream instance;
		     string instanceString;
		     string patternString;
		     stringstream pattern;
		     int stage = 0;
		     for (lineCount; lineCount < endLineCount; lineCount++) {
			     if (buffer[lineCount] == '<' || buffer[lineCount] == '>') {
				     //   if (workerId == 0 && i < 100) eFileStream << instance.str() << " " << pattern.str() << endl;
				     if (stage == 1) instanceString = instance.str();
				     else if (stage == 5) patternString = pattern.str();
				     if (stage == 6) break;
				     stage++;
				     continue;
			     }
			     
				if (stage == 1) {
					instance << buffer[lineCount];
					continue;
				}
				
				
				if (stage == 5) {
					pattern << buffer[lineCount];
					continue;
				}
			}
			//     if (workerId == 0 && i < 1000) eFileStream << "Instance : " << instanceString << " Pattern " << patternString << endl;
			if (stage != 6) continue;
			// string instanceString = instanceString;
			// string patternString = patternString;
			
			if (instanceString.empty()) continue;
			instanceString = getEndWord(instanceString);
			if (instanceString.empty()) continue;
			instanceString = filterString(instanceString);
			if (instanceString.empty()) continue;
			
			if (patternString.empty()) continue;
			patternString = getEndWord(patternString);
			if (patternString.empty()) continue;
			patternString = filterString(patternString);
			if (patternString.empty()) continue;
			
			// if (workerId == 0 && i < 100) eFileStream << "Instance : " << instance.str() << " Pattern: " << pattern.str() << endl;
			// Making sure that there are no duplicates
			duplicate = false;     
			patternRange = tmpMap.equal_range(instanceString);
			for (iterRange = patternRange.first; iterRange != patternRange.second; iterRange++) {
				string storedPattern = iterRange->second;
				if (patternString == storedPattern) duplicate = true;
			}
			
			if (duplicate == true) continue;
			// if (count == 15) errorStreamLog << " pattern: " << pattern << endl;
			tmpMap.insert(pair<string,string>(instanceString,patternString));
			
		} //  End of loop
		delete [] buffer;
		// inputFileStream.close();
	}else {
        if (workerId == 0) eFileStream << "Could not open the file " << endl;
    }
    if (workerId == 0)   eFileStream.close();
    ofstream outFileStream;
    stringstream typeFileStream;
    string typeFile;
    int numTypeLines = tmpMap.size();
    typeFileStream << outputDir << "type-" << workerId;
    typeFile = typeFileStream.str();
    outFileStream.open(typeFile.c_str());
    if (outFileStream) {
		int lineOffset = 300;
		int totalSize = numTypeLines * lineOffset + 20;
		buffer = new char[totalSize];
		memset(buffer,' ', totalSize);
		int index = 0;
		stringstream numLineStream;
		numLineStream  << numTypeLines;
		string numLineString = numLineStream.str();
		
		// Putting the number of lines at the beginning of the line
		for (int i = 0; i < 20; i++) {
			if (i < numLineString.size()) buffer[i] = numLineString[i];
			else buffer[i] = ' ';
			if (i == 19) buffer[i] = '\n';
		}
		index = 20;
		
		for (iter = tmpMap.begin(); iter != tmpMap.end(); iter = tmpMap.upper_bound(iter->first)) {
			string instanceName = iter->first;
			int lineIndex = index;
			
			// Instance or URI
			for (int i = 0; i < instanceName.size(); i++) {
				buffer[lineIndex++] = instanceName[i];
			}
			
			buffer[lineIndex++] = '*';
			
			// Type associated with the URI
			patternRange = tmpMap.equal_range(instanceName);
			for (iterRange = patternRange.first; iterRange != patternRange.second; iterRange++) { 
				string patternName = (*iterRange).second;
				for (int i = 0; i < patternName.size(); i++) {
					buffer[lineIndex++] = patternName[i];
				}
				buffer[lineIndex++] = '*';
				
			}
			buffer[lineIndex-1] = '\0';
			buffer[index + lineOffset -1] = '\n';
			
			index += lineOffset;
		}
		
		// Copy the char array into the output stream
		outFileStream.write(buffer,totalSize);
		delete [] buffer;
		outFileStream.close();
    }
	
}

void ReaderRDF::combineTypeFiles(string outputDir, string outputFileName, int numWorkers) {
	ifstream typeFileStream;
	int totalLineCount = 0;
	vector<int> fileSizes;
	
	// Determine find all the file sizes
	for (int i = 0; i < numWorkers; i++) {
		stringstream inputFile;
		ifstream inputFileStream;
		inputFile << outputDir << "type-" << i;
		inputFileStream.open(inputFile.str().c_str());
		int lineCount = readFileSize(inputFileStream,true);
		if (lineCount > 0) {
			totalLineCount += lineCount;
			fileSizes.push_back(lineCount);
		}
	}
	
	int lineSize = 300;
	int size = lineSize * totalLineCount;
	int index = 20;
	stringstream totalLineCountStream;
	totalLineCountStream << totalLineCount;
	string totalLineCountString = totalLineCountStream.str();
	ofstream outputFileStream;
	outputFileStream.open(outputFileName.c_str());
	if (outputFileStream ) {
		stringstream inputFile;
		int completeSize = size + 20;
		char *buffer = new char[completeSize];
		int offset = 20;
		char *bufferPointer = buffer + offset;
		
		for (int i =0; i < 20; i++) {
			if (i < totalLineCountString.size()) buffer[i] = totalLineCountString[i];
			else buffer[i] = '\0';
                        if (i == 19) buffer[i] = '\n';
		}
		
		for (int i = 0; i < numWorkers; i++) {
			ifstream inputFileStream;
			stringstream inputString;
			inputString << outputDir << "type-" << i;
			int currSize = fileSizes[i] * lineSize;
			inputFileStream.open(inputString.str().c_str());
			if (inputFileStream) {
				inputFileStream.seekg(offset,inputFileStream.beg);
				inputFileStream.read(bufferPointer,currSize);
				bufferPointer = buffer + currSize;
				inputFileStream.close();
			}
		} // End iteration over workers
		outputFileStream.write(buffer,completeSize);
		delete [] buffer;
		outputFileStream.close();
	} // If outputFileStream
}


void ReaderRDF::createTypeEntries(string inputTripleFile, string inputTypeFile, string outputDir, int workerId, int numWorkers, int size){
	ifstream inputTypeFileStream;
	ofstream outErrorStream;
	worker_Id = workerId;
	num_Workers = numWorkers;
	output_Dir = outputDir;
	
	if (workerId == 0) outErrorStream.open("createTypeEntries.txt");
	
	// Reading the entire type- file dataset
	if (workerId == 0) outErrorStream << " Create type entries 1 opening file " << inputTypeFile << endl;
	inputTypeFileStream.open(inputTypeFile.c_str());
	char *buffer;
	
	int lineTypeSize = 300;
	if (inputTypeFileStream) {
		int fileSize = readFileSize(inputTypeFileStream,true);
		if (workerId == 0) outErrorStream << " Create type entries:  File size " << fileSize << endl;
		
		int bufferSize = fileSize * lineTypeSize;;
		buffer = new char[bufferSize];
		int indexCount = 0;
		inputTypeFileStream.seekg(20,inputTypeFileStream.beg);
		inputTypeFileStream.read(buffer,bufferSize);
		inputTypeFileStream.close();
		for (indexCount; indexCount < bufferSize; indexCount += lineTypeSize) {
			string instance;
			instance.reserve(100);
			string pattern;
			pattern.reserve(100);
			vector<string> *patternList = new vector<string>();
			int stage = 0;
			int j = 0;
			for (j = indexCount; j < indexCount + lineTypeSize; j++) {
				
				if (buffer[j] == '\0') break;
				if (buffer[j] == '*') {
					if (stage > 0){
						patternList->push_back(pattern);
						pattern.clear();
						pattern.reserve(100);
					
					}
					stage++;
					continue;
				}
				
				if (stage == 0) {
					instance += buffer[j];
					continue;
				}
				
				if (stage >= 1) {
					pattern += buffer[j];
				}
			}
			
			if (patternList->size() > 0) {
				//  outErrorStream << " Instance: " << instanceString << " Pattern: " << (*patternList)[0] << endl;
				tmpMap.insert(pair<string,vector<string> *>(instance,patternList));
			}
		}
		
		delete [] buffer;
	}
	outErrorStream.close();
	
	loadMapFiles(inputTripleFile,size); 
	
	map<string,vector<string> *>::iterator iter;
	// deleting tmpMap
	for ( iter = tmpMap.begin(); iter != tmpMap.end(); iter++) {
		vector<string> * val = iter->second;
		if (val) delete val;
	}
	tmpMap.clear();
	
}

void ReaderRDF::loadMapFiles(string inputTripleFile, int size) {
	int lineSize = 400; // For map elements
	ifstream inputMapFileStream;
	stringstream inputTripleStream;
	ofstream outputFileStream;
	// int size = 25000000;
	inputTripleStream << inputTripleFile << "-" << size;
	ofstream nextErrorStream;
	nextErrorStream.open("loadMapFiles.txt",ofstream::out | ofstream::app);
	
	// Reading the normalized triple file , caching to 3 parallel vectors
	// nextErrorStream << " Opening file " << inputTripleStream.str() << endl;
	inputMapFileStream.open(inputTripleStream.str().c_str());
       
      
 
	int length;
	char * mapBuffer;
	if (inputMapFileStream) {
		int chunkSize = 0;
		int lCount = readFileSize(inputMapFileStream,false);
		chunkSize = int( (lCount / num_Workers) + .5);
		int offset = worker_Id * chunkSize * lineSize;
		if (worker_Id == num_Workers -1) {
			length = (lCount - (worker_Id * chunkSize)) * lineSize;
		} else {
			length = chunkSize * lineSize;
		}
		
		// nextErrorStream << "loadMapFiles:  buffer length " << length << " worker id " << worker_Id << endl;
		inputMapFileStream.seekg(offset,inputMapFileStream.beg);
		mapBuffer = new char[length];
		inputMapFileStream.read(mapBuffer,length);
		inputMapFileStream.close();
                // nextErrorStream.close();
                // return; 
		
		int index = 0;
		for (index; index < length; index += lineSize) {
			// stringstream subjectStream;
			string subject;
			// stringstream predicateStream;
			string predicate;
			// stringstream objectStream;
			string object;
			int stage = 0;
			// stringstream generalStream;
			string  valStream;
			// Reading subject predicate object for each line
			for (int j = index; j < index + lineSize; j++) {
				if (mapBuffer[j] == '<' || mapBuffer[j] == '>') {
					int currentStage = stage++;
					// if (workerId == 0 && j < 500) outErrorStream << generalStream.str()  << endl;
					if (currentStage == 0 || currentStage == 2 ) continue;           
					if (currentStage == 4 && valStream.size()) {valStream.clear(); break; }
					if (currentStage == 1) {
						//   if (workerId == 0 && j < 500) outErrorStream << " Subject " << subjectStream.str();
						subject = getEndWord(subject);
						if (subject.empty()) continue;
						subject = filterString(subject);
						if (subject.empty()) continue;
					} else if (currentStage == 3) {
						//    if (workerId == 0 && j < 500) outErrorStream << " Predicate " << predicateStream.str();
						predicate = getEndWord(predicate);
						if (predicate.empty()) continue;
						predicate = filterString(predicate);
						if (predicate.empty()) continue;
					} else if (currentStage == 5) {
						// if (workerId == 0 && j < 500) outErrorStream << " object " << objectStream.str();
						object = getEndWord(object);
						if (object.empty()) continue;
						object = filterString(object);
						if (object.empty()) continue;
 						if (subject.empty() || object.empty()) continue;
                                                if (subject == object) continue;
						subjectList.push_back(subject);
						predicateList.push_back(predicate);
						objectList.push_back(object);
						// if (worker_Id == 0) nextErrorStream << " subject " << subject << " predicate " << predicate << " object " << object << endl;
					}
					continue;
				}
				
				// generalStream << mapBuffer[j];
				
				if (stage == 0 || stage == 2) {
					continue;
				}
				if (stage == 4) {
					if (mapBuffer[j] != ' ') valStream +=  mapBuffer[j]; 
				}
				
				if (stage == 1 ) {
					subject +=  mapBuffer[j];
					continue;
				}
				
				if (stage == 3) {
					predicate += mapBuffer[j];
				}
				
				if (stage == 5) {
					object +=  mapBuffer[j];
				}
				
				
			}
		} // end of map chunk iteration
		delete [] mapBuffer;
	} // inputMapFileStream
	if (worker_Id == 0 ) nextErrorStream.close();
	createPatternFiles();
	

}

void ReaderRDF::createPatternFiles( ){
	
	// Creating the type-triple file for this worker node
	map<string,vector<string> *> typePatternMap;
	map<string,vector<string> *>::iterator iter;
	stringstream patternFileStream;
	
	//  vector<string> *subjectPatternList;
	//  vector<string> *objectPatternList;
	// ofstream nextErrorStream;
	// if (workerId == 0) nextErrorStream.open("createPatternFiles.txt");
	// if (workerId == 0) nextErrorStream << "Type triple file:  map size " << subjectList.size() << " tmpMap size " << tmpMap.size() <<  endl;
	for ( int i = 0; i < subjectList.size(); i++) {
		//string foundSubject;
		vector<string> *subjectPatternList = 0;
		// string foundObject;
		vector<string> *objectPatternList= 0;
		int count = 0;
		
		// Finding if the subject or object has a type
		for (iter = tmpMap.begin(); iter != tmpMap.end(); iter++) {
			
			string instance = iter->first;
			// if (workerId == 0) nextErrorStream << "Instance " << instance  << " subject " << subjectList[i] << " Object " << objectList[i] <<  endl;
			if (instance == subjectList[i] ) {
			
				subjectPatternList = iter->second;
			}
			
			if (instance == objectList[i]) {
			
				objectPatternList = iter->second;
			}
			if (objectPatternList != 0 && subjectPatternList != 0) break;
		}
		
		// <type pattern>*<predicate-string>*<?>
		if (subjectPatternList != 0 && objectPatternList != 0) {
			for (int p = 0; p < subjectPatternList->size(); p++) {
				for ( int j = 0; j < objectPatternList->size(); j++) {
					string subjectType = (*subjectPatternList)[p];
					string objectType = (*objectPatternList)[j];
					
					
					string actualPattern;
					map<string,vector<string>*>::iterator iter;
					actualPattern += subjectList[i];
					actualPattern += '*';
					actualPattern += predicateList[i];
					actualPattern += '*';
					actualPattern += objectList[i];
					
					vector<string> *patternList;

					
					// <Type Pattern> --pred-- < Type Pattern>  
					string typePattern;
					typePattern += subjectType;
					typePattern += "*";
				    typePattern += predicateList[i];
					typePattern +=  "*";
					typePattern += objectType;
					
					iter = typePatternMap.find(typePattern);
					if (iter == typePatternMap.end()) {
						patternList = new vector<string>();
						patternList->push_back(actualPattern);
					}else {
						patternList = iter->second;
                                                bool found = false;
                                                for (int count = 0; i < patternList->size(); count++) {
                                                   if ((*patternList)[count] == actualPattern) {
                                                      found = true;
                                                      break;
 
                                                   }
                                                }
						if (found == false) patternList->push_back(actualPattern);
					}
					typePatternMap.insert(pair<string,vector<string>*>(typePattern,patternList));
					
					// <Type Pattern> --pred-- < ?>
					string subjTypePattern;
					subjTypePattern += subjectType;
					subjTypePattern +=  "*";
					subjTypePattern += predicateList[i];
					subjTypePattern += "*";
					subjTypePattern += "?";
					iter = typePatternMap.find(subjTypePattern);
					if (iter == typePatternMap.end()) {
						patternList = new vector<string>();
						patternList->push_back(actualPattern);
					}else {
						patternList = iter->second;
                                                bool found = false;
                                                for (int count = 0; count < patternList->size(); count++) {
                                                   if ((*patternList)[count] == actualPattern) {
                                                      found = true;
                                                      break;
 
                                                   }
                                                }
						if (found == false) patternList->push_back(actualPattern);

					}
					typePatternMap.insert(pair<string,vector<string>*>(subjTypePattern,patternList));
					
					
					// <?> -- pred -- <Type pattern>
					string objTypePattern;
					objTypePattern += "?";
					objTypePattern += "*";
					objTypePattern += predicateList[i];
                                        objTypePattern += "*";
					objTypePattern += objectType;
					iter = typePatternMap.find(objTypePattern);
					if (iter == typePatternMap.end()) {
						patternList = new vector<string>();
						patternList->push_back(actualPattern);
					}else {
						patternList = iter->second;
                                                bool found = false;
                                                for (int count = 0; count < patternList->size(); count++) {
                                                   if ((*patternList)[count] == actualPattern) {
                                                      found = true;
                                                      break;
 
                                                   }
                                                }
						if (found == false) patternList->push_back(actualPattern);
					}
					typePatternMap.insert(pair<string,vector<string>*>(objTypePattern,patternList));
					
					
						
					
				}
			}
		}
	} // end iteration over pattern list 
	// if (workerId == 0) nextErrorStream.close();
	

	
	
	int lineCount = typePatternMap.size();
        if (lineCount < 1 ) return;
        stringstream lineStream;
        lineStream  << lineCount;
	string lineCountString = lineStream.str();
	int lineSize = 500;
	int bufferSize = lineSize * lineCount + 20;
	char *buffer;
	buffer = new char[bufferSize];
	int lineCounter = 0;
	
	// outErrorStream << " Num type pattern map " << typePatternMap.size() << endl;
	
	// if (workerId == 0) nextErrorStream.close();	
	
	
    int smallLineSize = 20;
	appendLine(buffer,lineCountString,smallLineSize,lineCounter);
	
	for (iter = typePatternMap.begin(); iter != typePatternMap.end(); iter++) {
		
		string pattern;
		pattern.reserve(lineSize);
		pattern = iter->first;
		pattern += '\0';
		
		vector<string> * tripleList = iter->second;
		for (int i = 0; i < tripleList->size(); i++) {
			pattern += (*tripleList)[i];
			if (i != tripleList->size() -1 )pattern += '\0';
		}
		appendLine(buffer,pattern,lineSize,lineCounter);
	}
	
	patternFileStream << output_Dir << "pattern-chunk-" << worker_Id;
	ofstream outputFileStream;
	outputFileStream.open(patternFileStream.str().c_str());
	outputFileStream.write(buffer,lineCounter);
	delete [] buffer;
	outputFileStream.close();
	
	
	// deleting
	for ( iter = typePatternMap.begin(); iter != typePatternMap.end(); iter++) {
		vector<string> * val = iter->second;
		if (val) delete val;
	}
	typePatternMap.clear();
	
	
}

void ReaderRDF::assemblePatternFiles(string inputDir, string inputTripleDataset, string inputPatternOffsets, int numWorkers) {

   int totalSize = 0;
   char *buffer;
   char *bufferPointer;
   int lineSize = 500;
   int bufferSize = 0;
   vector<int> fileSizes;
   ofstream errorFileStream;
   errorFileStream.open("assemblePatternFiles.txt");

   for (int i = 0; i < numWorkers; i++) {
      stringstream inputFile;
      ifstream inputFileStream;
      inputFile << inputDir << "pattern-chunk-" << i;
      inputFileStream.open(inputFile.str().c_str());
      if (inputFileStream) {
          int count = readFileSize(inputFileStream,true);
          if (count > 0) {
             totalSize += count;
             fileSizes.push_back(count);
          }
          inputFileStream.close();
      }
   }
   
   // ofstream outputFileStream;
   int index = 20;
   // outputFileStream.open(outputFile.c_str());
 
   bufferSize = lineSize * totalSize;     
   buffer = new char[bufferSize];
   bufferPointer = buffer;
   for ( int i = 0; i < numWorkers; i++) {
      ifstream inputFileStream;
      stringstream inputFile;
      inputFile << inputDir << "pattern-chunk-" << i;
      inputFileStream.open(inputFile.str().c_str());
      int currentSize = fileSizes[i] * lineSize;
      if (inputFileStream) {
	 inputFileStream.seekg(index,inputFileStream.beg);
	 inputFileStream.read(bufferPointer,currentSize);
	 bufferPointer += currentSize;
	 inputFileStream.close();
       }
   }

   // read each line chunk, input type pattern to triple
   int lineCount = 0;
   map<string,vector<string> *> fullPatternMap;
   map<string,vector<string> *>::iterator iter;
   for (lineCount = 0; lineCount < bufferSize; lineCount += lineSize) {
       int stage = 0;
       string typePattern;
       vector<string> * tripleList = new vector<string>();;
       stringstream tripleStream;
       for (int j = lineCount ; j< lineCount + lineSize; j++) {
           if (buffer[j] == '\0') {
               if (stage == 0) typePattern = tripleStream.str();
               else tripleList->push_back(tripleStream.str());
               stage++;
               tripleStream.str(std::string());
               tripleStream.clear();
               if (buffer[j+1] == '\0') break;
               continue;
           }
           
           tripleStream << buffer[j];
       }

       if (typePattern.size() != 0) {
          iter = fullPatternMap.find(typePattern);
          if (iter == fullPatternMap.end()) {
             fullPatternMap.insert(pair<string,vector<string>*>(typePattern,tripleList));
          }else {
             vector<string> *list = iter->second;
             for (int j = 0; j < tripleList->size(); j++) {
                 list->push_back((*tripleList)[j]);
             }
          }
          
       }
   }
   errorFileStream << "Calling store triples to file " << endl;
   errorFileStream.close();

   // Save the map file
   storeTriplesToFile(fullPatternMap,inputTripleDataset,inputPatternOffsets);

}

void ReaderRDF::storeTriplesToFile(map<string,vector<string> *> &tmpMap, string inputDataset, string inputOffset) {
    ofstream outputFileStream;
    char *buffer;
    char *secondBuffer;
    outputFileStream.open(inputDataset.c_str());
    int lineOffset =  500;
    int otherLineOffset = 100;
    int lineCount = tmpMap.size();
    int totalSize = lineCount * lineOffset;
    int secondTotalSize = lineCount * otherLineOffset + 20;
    ofstream errorFileStream;
    errorFileStream.open("storeTriplesToFile.txt");
    errorFileStream << "Line count " << lineCount << "Output dataset " << inputDataset << endl;
    errorFileStream.close();

    if (outputFileStream) {
       int index = 0;
       int secondIndex = 0;
       map<string,vector<string> *>::iterator iter;
		
	   // Allocation for queryFile and queryFileOffset
       buffer = new char[totalSize];
       secondBuffer = new char[secondTotalSize];
       memset(buffer,'\0',totalSize);
       memset(secondBuffer,'\0',secondTotalSize);
		
	   // Appending number of lines queryFileOffset buffer
	   stringstream lineStream;
	   lineStream << lineCount;
	   string lineCountString = lineStream.str();
	   appendLine(secondBuffer,lineCountString,20,secondIndex);

       for (iter = tmpMap.begin(); iter != tmpMap.end(); iter++) {
  
		   
		  // Retrieving the map pairs 
          string pattern = iter->first;
          vector<string> *tripleList = iter->second;
          string bufferStream;

		  // Storing the instance in the offset file
		  if (pattern.size() < otherLineOffset) appendLine(secondBuffer,pattern,otherLineOffset,secondIndex);
		   
		   pattern += '\0';
		   for (int i = 0; i < tripleList->size(); i++) {
			   pattern +=  (*tripleList)[i];
			   if (i != tripleList->size() -1) pattern += '\0';
		   }
		   
		   if (pattern.size() < lineOffset) appendLine(buffer,pattern,lineOffset,index);
		 

       } // end for loop

       outputFileStream.write(buffer,totalSize);
       delete [] buffer;
       outputFileStream.close();
    }

    ofstream newOutputStream;
    newOutputStream.open(inputOffset.c_str());
    if (newOutputStream) {
       newOutputStream.write(secondBuffer,secondTotalSize);
       newOutputStream.close();
    }

}
					
void ReaderRDF::appendLine(char *buffer, string & pattern, int offsetSize, int &counter) {
    for (int i = 0; i < offsetSize; i++) {
		if (i < pattern.size())
			buffer[counter++] = pattern[i];
		else {
			if (i == offsetSize -1) buffer[counter++] = '\n';
			else {
				buffer[counter++] = '\0';
			}
		}

	}
}

int ReaderRDF::readFileSize(ifstream & inputFileStream, bool top) {
	int fileSize = 0;
        ofstream testFileStream;
     //   testFileStream.open("testFileSize.txt");
	if (inputFileStream) {
		char *lineChar = new char[20];
		if (top == true) {
			inputFileStream.seekg(0,inputFileStream.beg);
		}else {
			inputFileStream.seekg(-20,inputFileStream.end);
		}
		inputFileStream.read(lineChar,20);
		
		string lineStream;
		for (int i = 0; i < 20 ; i++) {
	           lineStream += lineChar[i];
		}
		// testFileStream << " File Size " << lineStream.str() << endl;
		fileSize = atoi(lineStream.c_str());
		// inputFileStream.close();
		delete [] lineChar;
	}else {
              //  testFileStream << " Could not open file " << endl;
                testFileStream.close();
		return -1;
	}
	testFileStream.close();
	return fileSize;
}




// --------------------- Query functions  ---------------------------------
//

void ReaderRDF::cacheTriples(string inputFile) {
   ifstream inputFileStream;
   inputFileStream.open(inputFile.c_str());
   if (inputFileStream) {
       int size = readFileSize(inputFileStream,true);
       inputFileStream.seekg(20,inputFileStream.beg);
       char *buffer;
       int lineSize = 100;
       int length = lineSize * size;
       buffer = new char[length];
       inputFileStream.read(buffer,length);
       int index = 0;
       for (index; index < length; index += lineSize) {
           stringstream lineInput;
           for (int j = index ; j < index + lineSize; j++ ) {
                  if (buffer[j] == '\0') break;
                  lineInput << buffer[j]; 
           }
           tripleIndexList.push_back(lineInput.str());
       
       }
       delete [] buffer;
   }
}

void ReaderRDF::selectQueries(string queryFile, int numQueries, int *buffer) {
    // This function is only implemented by the root node
    ifstream inputFileStream;
    inputFileStream.open(queryFile.c_str());
    if (inputFileStream) {
       int size = readFileSize(inputFileStream,true);
       inputFileStream.close();
       set<int> querySelections;
       set<int>::iterator iter;
       int queryCount = 0;
       while (queryCount < numQueries) {
           int addedQuery;
           srand(clock());
           addedQuery = rand() % size;
           iter = querySelections.find(addedQuery);
           if (iter != querySelections.end()) {
              querySelections.insert(addedQuery);
              buffer[queryCount++] = addedQuery;
           } 
       }
       

    }

}

void printList(const vector<GraphStruct *> & nodeList, string outputFile) {
   ofstream outputFileStream;
   outputFileStream.open(outputFile.c_str());
   for (int i = 0; i < nodeList.size(); i++) {
       GraphStruct *node = nodeList[i];
       outputFileStream << node->id;
       for (int j = 0; j < node->outboundList.size(); j++) {
          GraphStruct *neighborNode = node->outboundList[j];
          outputFileStream << neighborNode->id; 
       }
       outputFileStream << endl;
   } 
   outputFileStream.close();
}


bool findMatches(GraphStruct *newNode, vector<GraphStruct *> & inNodes) {
   vector<string> splitVector;
   string delim = "*";
   bool foundMatch = false;
   // split(splitVector,newNode->triplePattern,is_any_of("*"));
   splitString(splitVector,newNode->triplePattern,delim);
   if (splitVector.size() != 4) return false;
   string outMatchSubject = splitVector[1];
   string outMatchReferent = splitVector[3];
   for (int i = 0 ; i < inNodes.size(); i++) {
      vector<string> tmpSplitVector;
      tmpSplitVector.clear();
      // split(tmpSplitVector,inNodes[i]->triplePattern,is_any_of("*"));
      splitString(tmpSplitVector,inNodes[i]->triplePattern,delim);
      if (tmpSplitVector.size() != 4) continue;
      string inMatchSubject = tmpSplitVector[1];
      string inMatchReferent = tmpSplitVector[3];
      if (outMatchReferent == inMatchSubject) {
         newNode->outboundList.push_back(inNodes[i]);
         foundMatch = true;
      }

      if (inMatchReferent == outMatchSubject) {
          inNodes[i]->outboundList.push_back(newNode);
          foundMatch = true;
      }

   }

   if (foundMatch == true) {
     inNodes.push_back(newNode);
   }
   return foundMatch;
}

void generateOutList(vector<GraphStruct *> & input, vector<GraphStruct *> & out) {
   int inputSize;
   int pickedNode;
   inputSize = input.size();
   while (input.size() != 0) {
      inputSize = input.size();
      srand(clock());
      pickedNode = rand() % inputSize;
      out.push_back(input[pickedNode]);
      input.erase(input.begin() + pickedNode);
   }
}

void addPatterns(string pattern, vector<string> & inGroup) {
   vector<string> splitVector;
   string delim = "*";
   // split(splitVector,pattern,is_any_of("*"));
   splitString(splitVector,pattern,delim);
   inGroup.push_back(splitVector[0]);
   inGroup.push_back(splitVector[2]);

}

void findPatterns(vector<string> & patternList, multimap<string,string,TypeComparator> &mapList, string tripleComponent, int index) {
   multimap<string,string,TypeComparator>::iterator iter;
   multimap<string,string>::iterator patternIter;
   pair< multimap<string,string>::iterator,multimap<string,string>::iterator > patternRange;
   
   for (iter = mapList.begin(); iter != mapList.end(); iter = mapList.upper_bound(iter->first) ) {
       string typeInstance = iter->first;
       if (typeInstance.empty()) continue;
       if (typeInstance == tripleComponent) {
           patternRange = mapList.equal_range(typeInstance);
           for (patternIter = patternRange.first; patternIter != patternRange.second; patternIter++){
                // if (index == 36421) errorFileStream << patternIter->second << endl;
                patternList.push_back(patternIter->second);
           }
       }

   }
}
void saveOpenPattern(string & outputDir, string & listFileName, string & subject, string & predicate, string & referent) {

   ofstream tripleFileStream;
   ifstream inputFileStream;
   string openMarker = "?";
   string star = "*";
   string baseName = openMarker + star + predicate + star + openMarker;
   string fileName = outputDir + baseName; 
   if (isOpen(fileName) == false ){
      ofstream outputFileStream;
      outputFileStream.open(listFileName.c_str(), ofstream::out | ofstream::app);
      outputFileStream << baseName << endl;
      outputFileStream.close();
   }
  

   tripleFileStream.open(fileName.c_str(), ofstream::out | ofstream::app);
   tripleFileStream << subject << " " <<  referent << endl;
   tripleFileStream.close();
}

void saveToFile(vector<string> & subjectList, vector<string> & referentList, string outputDir, string listFileName, string subject, string predicate, string referent){
  
   ofstream tripleFileStream;
   ifstream inputFileStream;
   string openMarker = "?";
   string star = "*";
 
   // Saving  <subjectType>*Predicate
   for (int i = 0; i < subjectList.size(); i++) {
      string baseName = subjectList[i] + star + predicate + star + openMarker;
      string fileName = outputDir + baseName; 
      if (isOpen(fileName) == false) {
	 ofstream outputFileStream;
	 outputFileStream.open(listFileName.c_str(), ofstream::out | ofstream::app);
	 outputFileStream << baseName << endl;
	 outputFileStream.close();
      }
      
      tripleFileStream.open(fileName.c_str(), ofstream::out | ofstream::app);
      tripleFileStream << subject << " " << referent <<  endl;
      tripleFileStream.close();
   }

   //  Saving <predicate>*<objectType>
   for (int i = 0; i < referentList.size(); i++) {
      string baseName = openMarker + star + predicate + star + referentList[i] ;
      string fileName = outputDir + baseName; 
      if (isOpen(fileName) == false) {
	 ofstream outputFileStream;
	 outputFileStream.open(listFileName.c_str(), ofstream::out | ofstream::app);
	 outputFileStream << baseName << endl;
	 outputFileStream.close();
      }
      
      tripleFileStream.open(fileName.c_str(), ofstream::out | ofstream::app);
      tripleFileStream << subject << " " << referent << endl;
      tripleFileStream.close();
   }

   for (int i = 0; i < subjectList.size(); i++) {
      for (int j = 0; j < referentList.size(); j++) {
	    if (subject[i] == referent[i]) continue;
	    string baseName = subjectList[i] + "*" + predicate + "*" + referentList[j];
	    string fileName = outputDir + baseName;
	    if (isOpen(fileName) == false) {
	       ofstream outputFileStream;
	       outputFileStream.open(listFileName.c_str(), ofstream::out | ofstream::app);
	       outputFileStream << baseName << endl;
	       outputFileStream.close();
	    }
	    
	    tripleFileStream.open(fileName.c_str(), ofstream::out | ofstream::app);
	    tripleFileStream << subject << " " << referent <<  endl;
	    tripleFileStream.close();	 
      }
   }

     
}

string getEndWord(string parsedString) {
    vector<string> newSplitVector;
    string empty;
    string delim = "/";
    // split(newSplitVector,parsedString,is_any_of("/"));
    splitString(newSplitVector,parsedString,delim);
    if (newSplitVector.size() > 0) {
      string & newString = newSplitVector[newSplitVector.size() -1];
      if (isalpha(newString[0]) == 0 || isalpha(newString[1]) == 0) return empty;
      return newString;
    }
    return parsedString;
}

string filterString(string parsedString) {
   string empty;
   string subString = parsedString.substr(0,2);
   if (isalpha(subString[0]) == 0 || isalpha(subString[1]) == 0) return empty;

   // prefix of word
   int findPos = parsedString.find("__");
   if (findPos != std::string::npos) {
       if (findPos <= 0) return empty;
       parsedString = parsedString.substr(0,findPos-1);
   }  

   // suffix of word
   findPos = parsedString.find("#");
   if (findPos != std::string::npos) {
      if (findPos >= parsedString.size() -1) return empty;
      parsedString = parsedString.substr(findPos + 1, parsedString.size() -1);
   }

   if (parsedString == "Organisation" || parsedString  == "Company" ) parsedString = "Organization";
   if (parsedString == "Thing") return empty;
   if (parsedString == "Agent") return empty;
   if (parsedString == "NaturalPerson" || parsedString == "SocialPerson" ) parsedString =  "Person";
   if (parsedString == "CreativeWork" || parsedString == "InformationEntity") parsedString = "Work";
   
   
   return parsedString;
}

bool isOpen(string &fileName) {
    ifstream inputStream;
    bool exists = false;
    inputStream.open(fileName.c_str());
    exists = inputStream.is_open();
    inputStream.close();
    return exists;

}

bool contains(string &source, string & target) {
   
   if (source.size() > target.size()) return false;
   int sourcePointer = 0;
   for (int i = 0; i < target.size(); i++) {
      if (target[i] == source[sourcePointer]) sourcePointer++;
      else {
         sourcePointer = 0;
         if (target[i] == source[sourcePointer]) sourcePointer++;
      }
      // If at the end
      if (sourcePointer = source.size() -1) return true;
   }
   return false;

}

void splitString(vector<string> & splitStrings, string &splitFile, string & delim) {
   
     int currentIndex = 0;
     string *currentString = new string();
     char frontMarker;
     char  backMarker;
     bool hasBackDelim = false;
     if (delim.size() == 2) {
        frontMarker = delim[0];
        backMarker = delim[1];
        hasBackDelim = true;
     }else if (delim.size() == 1) {
       frontMarker = delim[0];
     }else return;
     vector<string*> stringList;

     for (int i = 0; i < splitFile.size(); i++) {
        if (splitFile[i] == frontMarker) {
           // Increment to the next string
           splitStrings.push_back(*currentString);
           string *nextString = new string();
           stringList.push_back(nextString);
           if (i != splitFile.size() -1 ) {
             currentString = nextString;
           }
           continue;
        }
 
        if (hasBackDelim == true) {
	   if (splitFile[i] == backMarker) {
	      // Increment to the next string
	      splitStrings.push_back(*currentString);
	      string *nextString = new string();
              stringList.push_back(nextString);
              if (i != splitFile.size() -1) {
	       currentString = nextString;
              }
              continue;
	   }
          
        }
        string &curr = *currentString;
        curr = curr + splitFile[i];
        if (i == splitFile.size()-1) splitStrings.push_back(curr);
     }
     for (int i = 0; i < stringList.size(); i++) {
        string *stringPtr = stringList[i];
        delete stringPtr;
     }
     stringList.clear();
}

