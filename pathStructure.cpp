#include <string>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include "pathStructure.hxx"

using namespace std;

int readFileSize(ifstream & inputFileStream, bool top);



WorkerNode::WorkerNode() {
    generationMode = false;
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

        
        string lineChar;
        for (int i = 0; i < splitFile.size(); i++) {
            
            if (splitFile[i] == frontMarker) {
               splitStrings.push_back(lineChar);
               lineChar.clear();
               continue;
            }

            /*if (hasBackDelim == true && splitFile[i] == backMarker) {
               splitStrings.push_back(lineChar);
               lineChar.clear();
               break;
            }*/

            if (splitFile[i] != '\0') lineChar += splitFile[i];

        }
        splitStrings.push_back(lineChar);
    
}


void WorkerNode::loadTriples(string offsetFile, string dataset, int hashId, int numWorkers) {
    ifstream inputFileStream;
	datasetFile = dataset;
        ofstream errorFileStream;
	// errorFileStream.open("loadtriples.txt");
        inputFileStream.open(offsetFile.c_str());
        int lineSize = 100;
	int fileSize = readFileSize(inputFileStream,true);
        // errorFileStream << " Size " << fileSize << endl;
	int length = fileSize * lineSize;
	char *buffer = new char[length];
	inputFileStream.seekg(20,inputFileStream.beg);
	inputFileStream.read(buffer,length);
        inputFileStream.close();
        // generationMode = false;
        workerId = hashId;
	
	
	int index = 0;
	int count = 0;
	for (int index = 0; index < length; index += lineSize, count++) {
		string line;
                line.reserve(100);
		for (int j = index; j < (index + 100); j++) {
			if (buffer[j] != '\0') line +=  buffer[j];
			else {
                         line += buffer[j];
                         break;
                        }
		}
                // if (hashId == 0) errorFileStream << " Line " << line << endl;
                if (line.size() > 0) {
		   tripleList.push_back(line);
		   int currentIndex = count % numWorkers;
		   if (currentIndex == hashId) rootList.push_back(count);
                }
	}

	delete [] buffer;
        // inputFileStream.close();
        // errorFileStream.close();
        //ofstream errorOutputFile;
        // stringstream outFile;
        // outFile << " root - " << hashId << ".txt"; 
        // errorOutputFile.open(outFile.str().c_str());
        //
      

	for (int rootIndex = 0; rootIndex < rootList.size(); rootIndex++) {
		int tripleIndex = rootList[rootIndex];
		
		// Creating pathStructure and corresponding PathObject , adding to PathStructure->objectMap and 
		// pathStructure->fringeList
		PathStructure *pathStructure = new PathStructure(tripleList,datasetFile, tripleIndex, generationMode);
                pathStructures.push_back(pathStructure);	
        }
}

void WorkerNode::buildSubgraph(int maxRadius,string datasetName) {
    // Dirname is directory having th list of triple pattern files.
	
    ofstream outFileStream;
    outFileStream.open("buildSubgraph.txt",std::ofstream::out | std::ofstream::app);
	
	
	
    // outFileStream << "Machine " << workerId << "Initialized pathstructures- Number of root nodes:  " << rootList.size() <<  endl;
    
    maxRadius = 2;	
    for (int graphRadius = 1; graphRadius <= maxRadius; graphRadius++) {
        map<int,PathStructure*>::iterator iter;
        if (workerId == 0 ) outFileStream << " Number of path structures " << pathStructures.size() << endl;
        // Iterating across all the root indices
        for (int pathCount = 0; pathCount < pathStructures.size(); pathCount++) {
	
			PathStructure *pathStructure = pathStructures[pathCount];;
		        pathStructure->generationMode = generationMode;
	
			for (int i = 0; i < tripleList.size(); i++) {
				for (int j = 0; j < pathStructure->fringeList.size(); j++) {
					int fringeIndex = pathStructure->fringeList[j];
					if (fringeIndex != i ) {
                                                if (tripleList[i].empty()) continue;
 						// if (workerId == 0) outFileStream << " BuildSubgraph " << tripleList[i] << endl;
						pathStructure->makeConnection(fringeIndex,i,datasetName,workerId);
					}
					// iterate through all the path objects within this fringe node
				} // End fringeList iteration
			} // End of tripleList 

                        if (generationMode == true && graphRadius == maxRadius) {
                              if (workerId == 0) outFileStream << " Calling store-triples " << queryDirectory << " - " << workerId << endl;
                              pathStructure->storeTriples(queryDirectory,workerId);
                        }
			
			// clearing fringeList and moving siblingList to fringeList
			pathStructure->fringeList.clear();
			for (int j = 0; j < pathStructure->siblingList.size(); j++) {
				pathStructure->fringeList.push_back(pathStructure->siblingList[j]);
			}
			pathStructure->siblingList.clear();
        } // Path Structure iteration -- End this pathStructure's radius 
    } // End of radius iteration - incrementing the radius by one
    outFileStream.close();
}

void WorkerNode::inputGraphMatch(string inputGraphPath) {
   ifstream inputFileStream;
   inputFileStream.open(inputGraphPath.c_str());
   string line;
   string delim = " ";
   while(getline(inputFileStream,line)) {
      vector<string> splitVector;
      // split(splitVector,line, is_any_of(" "));
      splitString(splitVector,line,delim);
      int startWorkerNode = atoi(splitVector[0].c_str());
   }
}

// Query generation functions
void WorkerNode::generateQuery(string queryDir, string offsetFile, string tripleDataset, int id, int numWorkers, int numRounds) {
     maxRadius = numRounds;
     generationMode = true;
     queryDirectory = queryDir;
     workerId = id;
     loadTriples(offsetFile,tripleDataset,workerId,numWorkers);
     buildSubgraph(maxRadius,tripleDataset);     
     // map<int,PathStructure*>::iterator iter;
}


void WorkerNode::combineQueryFiles(string queryDir, string combinedFile, int numWorkers) {
    int lineSize = 500;
    char *buffer;
    char *bufferPointer;
    vector<int> fileSizes;
    int totalSize = 0;
    int bufferSize = 0;
 
    for (int i = 0; i < tripleList.size(); i++) {
       stringstream inputFile;
       ifstream inputFileStream;
       inputFile << queryDir << "query-" << i;
       inputFileStream.open(inputFile.str().c_str());
       if (inputFileStream) {
          int count = readFileSize(inputFileStream,false);
          if (count > 0)  {
             totalSize += count;
             fileSizes.push_back(count);
          }
          inputFileStream.close();
       }
    }


    int index = 20;
    ofstream outputFileStream;
    outputFileStream.open(combinedFile.c_str());
    
    bufferSize = lineSize * totalSize + 20;
    buffer = new char[bufferSize];
    stringstream outputCountStream;
    outputCountStream << totalSize;
    string outputCount = outputCountStream.str();
    for ( int i = 0; i < totalSize; i++) {
       if ( i < outputCount.size()) 
          buffer[i] = outputCount[i];
       else {
          if ( i < 19) 
          buffer[i] = ' ';
          else buffer[i] = '\n';
       }
    }

    bufferPointer = buffer;
    bufferPointer += 20;
    for (int i = 0; i < tripleList.size(); i++) {
       ifstream inputFileStream;
       stringstream inputFile;
       inputFile << queryDir << "query-" << i;
       int currentSize = fileSizes[i] * lineSize;
       if (inputFileStream ) {
          inputFileStream.seekg(index,inputFileStream.beg);
          inputFileStream.read(bufferPointer,currentSize);
          bufferPointer += currentSize;
          inputFileStream.close();
       }
    }
  
    outputFileStream.write(buffer,bufferSize);
    delete [] buffer;
    outputFileStream.close();
  
    

}


int * WorkerNode::makeBid(int *sendBuffer, string queryFile, int numQueries) {
	
    ifstream inputFileStream;
    inputFileStream.open(queryFile.c_str());
	if (answers) delete [] answers;
	answers = new int[numQueries];
	int lineSize = 500;
    if (inputFileStream) {
		for (int i = 0; i < numQueries; i++) {
			int queryIndex = sendBuffer[i];
			int offset = queryIndex*lineSize;
			char *lineBuffer = new char[lineSize];
			inputFileStream.seekg(offset,inputFileStream.beg);
			inputFileStream.read(lineBuffer,lineSize);  
			inputFileStream.close(); 
			
			int stage = 0;
			vector<LinkedPairs*> linkedPairList;
			LinkedPairs *lpairs;
			string sourceId;
			string targetId;
			string sourceType;
			string targetType;
			for (int index = offset; index < offset + lineSize; index++) {
			    if (lineBuffer[index] == '-') {
					stage++;
					continue;
				}
				if (lineBuffer[index] == '\0'){
					LinkedPairs *lpairs = new LinkedPairs();
					lpairs->sourceId = atoi(sourceId.c_str());
					lpairs->targetId = atoi(targetId.c_str());
					lpairs->sourceType = atoi(sourceType.c_str());
					lpairs->targetType = atoi(targetType.c_str());
					linkedPairList.push_back(lpairs);
					sourceId.clear();
					targetId.clear();
					sourceType.clear();
					targetType.clear();
					stage == 0;
					continue;
				}
				if (stage == 0) {
					sourceId += lineBuffer[index];
					continue;
				}
				if (stage == 1) {
				    targetId += lineBuffer[index];
					continue;
				}
				
				if (stage == 2) {
					sourceType += lineBuffer[index];
					continue;
				}
				
				if (stage == 3) {
				    targetType += lineBuffer[index];
					continue;
				}
		 }
			delete [] lineBuffer;
			int numJoins = compareList(linkedPairList);
			answers[i] = numJoins;
			
		} // end num files
    }
    return answers;
}

int WorkerNode::compareList(vector<LinkedPairs*> &queryList) {
	int largestCount = 0;
	int structureIndex = 0;

	map<int,PathStructure*>::iterator iter;
	for (int pathCount = 0; pathCount < pathStructures.size(); pathCount++) {
		PathStructure * pathStructure = pathStructures[pathCount];
		
		int localSize = 0;
		PatternObject *po = pathStructure->objectMap.find(pathStructure->rootId)->second;
                
		
		for (int j = 0; j < po->pathObjectList->size(); j++) {
			PathObject *pathObj = (*po->pathObjectList)[j];
			
		    for (int k = 0; k < pathObj->pathContents->size(); k++) {
		    	 LinkedPairs *pairs = (*pathObj->pathContents)[k];
				for (int i = 0; i < queryList.size(); i++) {
					LinkedPairs *inputPair = queryList[i];
					if (pairs->sourceId == inputPair->sourceId &&
						pairs->targetId == inputPair->targetId &&
						pairs->sourceType == inputPair->sourceType &&
						pairs->targetType == inputPair->targetType) localSize++;
				}
		    }
		}
		
		if (localSize > largestCount ) {
			largestCount = localSize;
		}
	}
	
	return largestCount;
	
}



//---------------- PathStructure -----------------------------------

PathStructure::PathStructure(vector<string> &list, string datasetName, int id, bool mode)  {
	string keyName = list[id];
        rootId = id;
        generationMode = mode;
        tripleList = &list;
	
	// Creates and loads up first DataInstance and adds first Link to pathContents
	PathObject *pathObject = new PathObject(rootId, datasetName, 0, mode, &objectMap);
	
	vector<PathObject *> *pathObjectList = new vector<PathObject *>();
	pathObjectList->push_back(pathObject);
        PatternObject *po = new PatternObject(pathObjectList);
        po->subj = pathObject->subj;
        po->pred = pathObject->pred;
        po->obj = pathObject->obj;
         
	objectMap.insert(pair<int,PatternObject*>(id,po));
	
	
	fringeList.push_back(pathObject->patternId);
	
	for (int i = 0; i < tripleList->size(); i++) {
		if (i != rootId) {
		   PatternObject *po = new PatternObject();
		   objectMap.insert(pair<int,PatternObject*> (i,po));
		}
	}
}

void PathStructure::updateSubgraph(PathObject *targetPathObject) {

   set<int> updates;
   set<int>::iterator updateIter;

   for (int i = 0; i < targetPathObject->pathContents->size(); i++) {
       LinkedPairs *pairs = (*targetPathObject->pathContents)[i];
       int sourceId = pairs->sourceId;
       updateIter = updates.find(sourceId);
       if (updateIter == updates.end()) {
          PatternObject *patternObj = objectMap.find(sourceId)->second;
          if (patternObj != 0) {
            vector<PathObject *> *pathVector = patternObj->pathObjectList;
            pathVector->push_back(targetPathObject);
          }
          updates.insert(sourceId);
       }

       int targetId = pairs->targetId;
       updateIter = updates.find(targetId);
       if (updateIter == updates.end()) {
          PatternObject *patternObj = objectMap.find(targetId)->second;
          if (patternObj != 0) {
             vector<PathObject *> *pathVector = patternObj->pathObjectList;
             pathVector->push_back(targetPathObject);
          }
          updates.insert(targetId);
       }

   } 

}

void PathStructure::storeTriples(string queryDir, int workerId) {

  
   stringstream queryFile;
   queryFile << queryDir << "query-" << rootId;
   ofstream outputFileStream;
   outputFileStream.open(queryFile.str().c_str());
   int lineSize = 500;
   int pathCount = 0;
   ofstream outputStream;
   if (workerId == 0) outputStream.open("storeTriple.txt");
   if (workerId == 0) outputStream << " Starting to open " << queryFile.str().c_str() << endl;

   if (outputFileStream) {
         if (workerId == 0) outputStream << " Opened " << queryFile.str().c_str() << "root Id " << rootId << endl;
         if (workerId == 0) outputStream.close();
	  PatternObject *po = objectMap.find(rootId)->second;
	  for (int j = 0; j < po->pathObjectList->size(); j++) {
             PathObject *pathObj = (*po->pathObjectList)[j];
             if (pathObj->pathContents->size() == 0) continue;
             stringstream lineStream;
             for (int k = 0; k < pathObj->pathContents->size(); k++) {
                LinkedPairs *pairs = (*(pathObj->pathContents))[k];
                lineStream << pairs->sourceId << "-" << pairs->targetId << "-" << pairs->sourceType << "-" << pairs->targetType << '\0';
                // delete pairs;
              }
           
	      string lineString = lineStream.str();
	      for (int l = 0; l < lineSize; l++) {
		    if (l < lineString.size() ) {
		       outputFileStream << lineString[l]; 
		    }else {
		       if (l == lineSize -1) outputFileStream << '\n';
		       else outputFileStream << '\0';
		    }
	      }
           
             // delete pathObj;
          }
          
         //  delete pathStructure; 
      
	 stringstream countStream;
	 countStream << pathCount;
	 string countString = countStream.str();
	 for (int i = 0; i < 20; i++) {
	     if (countString.size() < i) {
		outputFileStream << countString[i];
	     }else {
		if (i == 19) outputFileStream << '\n';
		else outputFileStream << '\0';
	     }
	 }
	 outputFileStream.close();
   }
}

void PathStructure::makeConnection(int sourceId, int targetId, string datasetName, int workerId) {
	
	// Source Id -- nodes already within the subgraph e.g fringe or sibling nodes
	PatternObject *po = objectMap.find(sourceId)->second;
        ofstream outputStream;
        // if (workerId == 0) outputStream.open("connectAppend.txt", std::ofstream::out | std::ofstream::app);
        // if (workerId == 0) outputStream << " Size of Path object list " << pathObjectList->size() << " tripleList " << (*tripleList)[targetId] << endl;
        // outputStream.close();	
	string delim = "*\0";
	for (int iter = 0; iter < po->pathObjectList->size(); iter++) { 
		PathObject *sourcePathObject = (*po->pathObjectList)[iter];
                sourcePathObject->workerId = workerId;
                // if (workerId == 0) outputStream << " Data " << sourcePathObject->containsData << endl;
		if (sourcePathObject->containsData == false) continue;
		vector<string> splitVector;
		string line;

		// Retrieving the pattern name
		splitString(splitVector,(*tripleList)[targetId],delim);
               // if (workerId == 0) outputStream << "Size " << splitVector.size() << " string: " << (*tripleList)[targetId] << endl;
               // if (workerId == 0) outputStream.close();
		if (splitVector.size() != 3 ) continue;
		string subjName = splitVector[0];
		string predName = splitVector[1];
		string objName = splitVector[2];
		
		sourcePathObject->connectAppendPropagate(this, targetId, subjName,objName, datasetName);
		
	} // End pathObject iteration	 
        if (workerId == 0) outputStream.close();
}






//---------------- Path Object -----------------------------------

PathObject::PathObject(int pId, string datasetName,int mode, bool genMode, map<int,PatternObject*> *objMap) {
        // All non-root path objects start with pathContents of size 0;
        //
        objectMap = objMap;
	pathContents = new vector<LinkedPairs*> ();
        if (mode == 0) {
           LinkedPairs *linkedPair = new LinkedPairs();
           linkedPair->sourceId = pId;
           linkedPair->targetId = pId;
           pathContents->push_back(linkedPair);
        }

	patternId = pId;
	dataInstance = new DataInstance();
	containsData = dataInstance->initialize(pId, datasetName, mode, genMode);
        if (mode == 0) {
           ColumnData *cData = dataInstance->columnList[0];
           subj = cData->subjTypeName;
           pred = cData->predicateName;
           obj = cData->objTypeName;
        }
        
}

bool PathObject::checkDuplicateConnections(LinkedPairs *target) {
    
     for (int k = 0; k < pathContents->size(); k++) {
	     LinkedPairs *source = (*pathContents)[k];
             if ( (source->sourceId == target->sourceId)  && (source->targetId == target->targetId ) &&
                  (source->sourceType == target->sourceType) && (source->targetType == target->targetType))
                return true;
     }
     return false;

}

void PathObject::join(PathObject *sourcePathObject) {
	
	vector<string> tmpSubject;
	vector<string> tmpObject; 
	
	// Appending the last linkedPair item within the path contents
	LinkedPairs *linkedPairs = (*pathContents)[pathContents->size() -1];
	
	// Retrieving the source node column data
        vector<ColumnData *> & sourceColumnList = sourcePathObject->dataInstance->columnList;
	ColumnData *sourceColumnData = sourceColumnList[sourceColumnList.size() -1];

	
	vector<int> left;
	vector<int> right;
	
	// Finding the common index between the source
	if (linkedPairs->sourceType == 0) {
		if (linkedPairs->targetType == 0) {
                    // subj - subj 
		    merge(sourceColumnData->subjectCol,*(dataInstance->tmpSubjList),left,right);
		}else {
		    // Subj - obj
	            merge(sourceColumnData->subjectCol,*(dataInstance->tmpObjList),left,right);
		}
		
	}else if (linkedPairs->sourceType == 1 ) {
		if (linkedPairs->targetType == 0) {
			
			merge(sourceColumnData->objectCol,*(dataInstance->tmpSubjList),left,right);
		}else {
             
			merge(sourceColumnData->objectCol,*(dataInstance->tmpObjList),left,right);
		}
	}

        if (left.size() == 0) {
           containsData = false;
           return;
        }
    
	// Copying the source meta data to the target path object
	for (int i = 0; i < sourceColumnList.size(); i++) {
		ColumnData *targetData = new ColumnData;
		ColumnData *sourceData = sourceColumnList[i];
		targetData->subjTypeName = sourceData->subjTypeName;
		targetData->objTypeName = sourceData->objTypeName;
		targetData->predicateName = sourceData->predicateName;
		targetData->tripleId = sourceData->tripleId;
		dataInstance->columnList.push_back(targetData);
	}
	
        // copy the intersecting source data to the corresponding path object
	for (int i = 0; i < sourceColumnList.size(); i++) {
		ColumnData *data = sourceColumnList[i];
		ColumnData *targetData = dataInstance->columnList[i];
		
		int index = 0;
		for (int j = 0; j < data->subjectCol.size(); j++) {
			if (j == left[index]) {
				targetData->subjectCol.push_back(data->subjectCol[j]);
				targetData->objectCol.push_back(data->objectCol[j]);
				index++;            
			}
		}
	}   
	
	int index = 0;
	ColumnData *targetColumnData = new ColumnData();
	targetColumnData->subjTypeName = subj;
	targetColumnData->objTypeName = obj;
	targetColumnData->predicateName = pred;
	targetColumnData->tripleId = patternId;
	vector<string> *subjList = dataInstance->tmpSubjList;
	vector<string> *objList = dataInstance->tmpObjList;
	
	for (int i = 0; i < subjList->size(); i++) {
		if (i == right[index]){
			targetColumnData->subjectCol.push_back((*subjList)[i]);
			targetColumnData->objectCol.push_back((*objList)[i]);
			index++;
		}
	}
	
	dataInstance->columnList.push_back(targetColumnData);   
	subjList->clear();
	objList->clear();
}

void PathObject::merge(vector<string> & sourceList, vector<string> & targetList, vector<int> & left, vector<int> & right) {
	
	// Finding the common merge indices
	for (int i = 0; i < sourceList.size(); i++) {
		for (int j = 0; j < targetList.size(); j++) {
			if (sourceList[i] == targetList[j]) {
				left.push_back(i);
				right.push_back(j);
			}
		}
	}
}

void PathObject::savePathData(int machineId) {
	
	stringstream dir;
	dir << "Machine" << "-" << machineId << "/";;
	string dirName = "/projects/ExaHDF5/mlewis/wiki/output/" + dir.str();
	vector<string> fileList;
	// Create the file name
	for (int i = 0; i < dataInstance->columnList.size(); i++) {
		stringstream fileName;
		ColumnData *colData = dataInstance->columnList[i];
		fileName << colData->subjTypeName << "-" << colData->predicateName << "-" <<colData->objTypeName;
		fileList.push_back(fileName.str());
	}
	
	
	for (int i = 0; i < dataInstance->columnList.size(); i++) {
		string fileName = dir.str() + fileList[i];
		ofstream outputFileStream; 
		outputFileStream.open(fileName.c_str(), std::ofstream::out | std::ofstream::app);
		ColumnData *colData = dataInstance->columnList[i];
		
		// Every column has the same number of rows
		for ( int j = 0 ; j < colData->subjectCol.size(); j++) {
			outputFileStream << colData->subjectCol[j] << "--" << colData->predicateName << "--" << colData->objTypeName << endl;
		}
		outputFileStream.close();
	}
}

void PathObject::connectAppendPropagate(PathStructure *pathStructure, int targetPatternId, string subjInst, string objInst, string datasetName) {
    
    vector<LinkedPairs *> *linkedPairList = new vector<LinkedPairs*>();
    ofstream outputFileStream;
    bool hasSubjMatch = false;
    bool hasSubjObjMatch = false;
    bool hasObjSubjMatch = false;
    set<int> updates;
    set<int>::iterator updateIter;

    // Determine pattern connection types
    if (subj == subjInst || subj == "?" || subjInst == "?" ) hasSubjMatch = true;
    if (subj == objInst || subj == "?" || objInst == "?" ) hasSubjObjMatch = true;
    if (obj == subjInst || obj == "?" || subjInst == "?" ) hasObjSubjMatch = true;

    for (int i = 0; i < pathContents->size(); i++) {
      LinkedPairs *pairs = (*pathContents)[i];
      if (pairs->sourceId == targetPatternId || pairs->targetId == targetPatternId) return;
    }

    for (int i = 0; i < pathContents->size(); i++) {
       LinkedPairs *pairs = (*pathContents)[i];
       int sourceId = pairs->sourceId;
       int targetId = pairs->targetId;
       int primarySource;
       if (sourceId == targetPatternId || targetId == targetPatternId) continue;
       primarySource = targetId;
       updateIter = updates.find(primarySource);
       if (updateIter == updates.end()) {
          findMatch(primarySource,subjInst,objInst,hasSubjMatch,hasSubjObjMatch,hasObjSubjMatch);

          if (hasSubjMatch == true) {
             PathObject *pathObject = new PathObject(targetPatternId,datasetName,1,generationMode,objectMap);
             addPathContent(pathObject, primarySource, targetPatternId, 0);
             pathStructure->updateSubgraph(pathObject);
          }

          if (hasSubjObjMatch == true) {
             PathObject *pathObject = new PathObject(targetPatternId,datasetName,1,generationMode,objectMap);
             addPathContent(pathObject, primarySource, targetPatternId, 1);
             pathStructure->updateSubgraph(pathObject);
          }

          if (hasObjSubjMatch == true) {
             PathObject *pathObject = new PathObject(targetPatternId,datasetName,1,generationMode,objectMap);
             addPathContent(pathObject, primarySource, targetPatternId, 2);
             pathStructure->updateSubgraph(pathObject);
          }
       }
       
       primarySource = sourceId;
       updateIter = updates.find(primarySource);
       if (updateIter == updates.end()) {
          findMatch(primarySource,subjInst,objInst,hasSubjMatch,hasSubjObjMatch,hasObjSubjMatch);
          if (hasSubjMatch == true) {
             PathObject *pathObject = new PathObject(targetPatternId,datasetName,1,generationMode,objectMap);
             addPathContent(pathObject, primarySource, targetPatternId, 0);
             pathStructure->updateSubgraph(pathObject);
          }

          if (hasSubjObjMatch == true) {
             PathObject *pathObject = new PathObject(targetPatternId,datasetName,1,generationMode,objectMap);
             addPathContent(pathObject, primarySource, targetPatternId, 1);
             pathStructure->updateSubgraph(pathObject);
          }

          if (hasObjSubjMatch == true) {
             PathObject *pathObject = new PathObject(targetPatternId,datasetName,1,generationMode,objectMap);
             addPathContent(pathObject, primarySource, targetPatternId, 2);
             pathStructure->updateSubgraph(pathObject);
          }
       }

   }
}

void PathObject::addPathContent(PathObject *pathObject, int sourceId, int targetId, int type) {

    for (int i  = 0; i < pathContents->size(); i++) {
        LinkedPairs *tmpPair = (*pathContents)[i];
        if  (tmpPair->sourceId == tmpPair->targetId) continue;
        pathObject->pathContents->push_back(tmpPair);
    }

    LinkedPairs *linkedPair = new LinkedPairs();
    createLinkedPair(linkedPair, sourceId, targetId, type);
    pathObject->pathContents->push_back(linkedPair);
}

void PathObject::findMatch(int sourceId, string tSubjName, string tObjName, 
			   bool &hasSubjMatch, bool & hasSubjObjMatch, bool & hasObjSubjMatch)  
{
    hasSubjMatch = false;
    hasSubjObjMatch = false;
    hasObjSubjMatch = false;

    PatternObject *patternObj = objectMap->find(sourceId)->second;
    if (patternObj == 0) return;

    subj = patternObj->subj;
    obj = patternObj->obj;

    if (subj == tSubjName || subj == "?" || tSubjName == "?") hasSubjMatch = true;
    if (subj == tObjName || subj == "?" || tObjName == "?") hasSubjObjMatch = true;
    if (obj == tSubjName || obj == "?" || tSubjName == "?") hasObjSubjMatch = true;

}

void PathObject::createLinkedPair(LinkedPairs *linkedPair, int sourceId, int targetId, int type) { 

    // 0 = subj-subj , 1 = subj-obj, 2 = obj-subj
    if (type == 0) {
	    LinkedPairs * linkedPairs = new LinkedPairs();
	    linkedPairs->sourceId = sourceId;
	    linkedPairs->targetId = targetId;
	    linkedPairs->sourceType = 0;
	    linkedPairs->targetType = 0;
	    pathContents->push_back(linkedPairs);
    }
    
    else if (type == 1){
	    LinkedPairs * linkedPairs = new LinkedPairs();
	    linkedPairs->sourceId = sourceId;
	    linkedPairs->targetId = targetId;
	    linkedPairs->sourceType = 0;
	    linkedPairs->targetType = 1;
	    pathContents->push_back(linkedPairs);
    }
    
    else if (type == 2) {
	    LinkedPairs * linkedPairs = new LinkedPairs();
	    linkedPairs->sourceId = sourceId;
	    linkedPairs->targetId = targetId;
	    linkedPairs->sourceType = 1;
	    linkedPairs->targetType = 0;
	    pathContents->push_back(linkedPairs);
    }

  
}


//---------------------- Data Instance -----------------------------

bool DataInstance::initialize(int patternId, string datasetName, int mode, bool genMode) {
  ColumnData *columnData = new ColumnData();
	columnData->tripleId = patternId;
	bool val = loadPatternFile(columnData,patternId,datasetName,mode,genMode);
	columnList.push_back(columnData);
        
	return val;
}

bool DataInstance::loadPatternFile(ColumnData *colData, int patternId, string datasetName, int mode, bool genMode) {
	
	ifstream inputFileStream;
	bool hasData = false;
	inputFileStream.open(datasetName.c_str());
        ofstream rootVerificationStream;
        stringstream fileName;
        string base = "/projects/ExaHDF5/mlewis/wiki/output/roots/";
        fileName << base << "root-" << patternId;
        rootVerificationStream.open(fileName.str().c_str());

	if (inputFileStream){
		int offset = patternId * 500;
	        inputFileStream.seekg(offset,inputFileStream.beg);
		char *buffer;
		int length = 500;
		buffer = new char[length];
		inputFileStream.read(buffer,length);
		inputFileStream.close();
		string subjectStream;
                subjectStream.reserve(150);
		string predicateStream;
                predicateStream.reserve(150);
		string objectStream;
                objectStream.reserve(150);
		int stage = 0;
		int starCount = 0;
		
		for (int i = 0;i < 500; i++) {
			if (buffer[i] == '\0') {
				if (stage == 0){
                                        // Pattern name
					colData->subjTypeName = subjectStream;
					colData->predicateName = predicateStream;
					colData->objTypeName = objectStream;
                                   //     rootVerificationStream << subjectStream << "--" << predicateStream << "--" << objectStream << endl;
					predicateStream.clear();
                                        predicateStream.reserve(150);
  				        subjectStream.clear();
                                        subjectStream.reserve(150);
                                        objectStream.clear();
					objectStream.reserve(150);
                                        hasData = true;
                                        if (genMode == true) break;
					
				}else {
					if (mode == 0){
						colData->subjectCol.push_back(subjectStream);
						colData->objectCol.push_back(objectStream);
					}else if (mode ==1) {
						tmpSubjList->push_back(subjectStream);
						tmpObjList->push_back(objectStream);
					}
                                        rootVerificationStream << subjectStream << "--" << objectStream << endl;
					predicateStream.clear();
                                        predicateStream.reserve(150);
  				        subjectStream.clear();
                                        subjectStream.reserve(150);
                                        objectStream.clear();
					objectStream.reserve(150);
					hasData = true;
				}
				
				starCount = 0;
                                if (i +1 < 500) {
                                   if (buffer[i+1] == '\0') break;
                                }
				continue;
			}
			
			if (buffer[i] == '*') {
			    starCount++;
				continue;
			}
			
			if (stage == 0) {
				if (starCount == 0) subjectStream += buffer[i];
				if (starCount == 1) predicateStream += buffer[i];
				if (starCount == 2) objectStream += buffer[i];
			}else {
				if (starCount == 0) subjectStream += buffer[i];
                               //  if (starCount == 1) predicateStream << buffer[i];
				if (starCount == 2) objectStream += buffer[i];
			}
		}
		
	}
        rootVerificationStream.close();
	return hasData;
	
}

// helper functions



int readFileSize(ifstream & inputFileStream, bool top) {
	int fileSize = 0;
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
		
		fileSize = atoi(lineStream.c_str());
		// inputFileStream.close();
		delete[] lineChar;
	}else {
		return -1;
	}
	return fileSize;
}



