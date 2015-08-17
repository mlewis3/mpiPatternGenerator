#include <mpi.h>
#include <sys/time.h>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include "rdfReader.hxx"
#include "pathStructure.hxx"

using namespace std;

// ReaderRDF * ReaderRDF::readerInstance = 0;
// TypeList * TypeList::typeInstance = 0;
double *timeStamps;
int workerStatus;
void createSubjTypeFiles(int id, int numWorkers, int numLines);
void combineSubjectTypeFiles(int processId, int numWorkers);
void createTypeTripleFiles(int id, int numWorkers, int numLines);
void combineTypeTripleFiles(int id, int numWorkers);
void loadRoots(int id,int numWorkers);
void checkStatus(int *stat, int id, int stage, int numWorkers);
void getTime(int stage, int id, bool timeMarker);
struct timeval timeValue;
double *timeArray;
int *globalStatus;
ReaderRDF *reader;
int numTypeLines;
int numMapLines;

vector<WorkerNode *> workerNodeList;
WorkerNode *workerNode;
string debugFile = "checkStatus.txt";
string timeFile = "Output.txt";

// Normalized datasets 
string inputTypeFile = "/projects/ExaHDF5/mlewis/wiki/database/instance_types_en.nt";
string inputMapFile = "/projects/ExaHDF5/mlewis/wiki/database/mappingbased_properties_en.nt";

// directory for the subject-type files --- typeFileStream << outputDir << "type-" << workerId;
string outputTypeDir = "/projects/ExaHDF5/mlewis/wiki/database/typeFiles/";

// file name for the collected subj-type file
string instancePatternDataset = "/projects/ExaHDF5/mlewis/wiki/output/instPatternDataset.txt";

// Input triple datasets---  inputFile << inputTripleDir << "pattern-chunk-" << i;
string inputTripleDir = "/projects/ExaHDF5/mlewis/wiki/database/mapFiles/";

// All the type triples in the datase
string inputTripleDataset = "/projects/ExaHDF5/mlewis/wiki/output/typeTriples.txt";

// Triple pattern offset file
string inputPatternOffsets = "/projects/ExaHDF5/mlewis/wiki/output/tripleOffsets.txt";

int main(int argc, char *argv[]) {

   MPI::Init (argc,argv);
   int numWorkers;
   int id;
   int numberGenerator;
   int processes = MPI::COMM_WORLD.Get_size();
   int numTypeLines = atoi(argv[1]);
   int numMapLines = atoi(argv[2]);
   numWorkers = processes -1;
   id = MPI::COMM_WORLD.Get_rank();
   workerNode = new WorkerNode();
   reader = new ReaderRDF();
   timeArray = new double[10];
   int numLines = 25000000;
   //  ofstream outputStatStream;
   
   //i if (id == 0) {
   //   outputStatStream.open(timeFile.c_str());
   // }

   workerStatus = 0;
   globalStatus = new int[processes];
   for (int i = 0; i < processes; i++) 
      globalStatus[i] = 0;


   // Stage one create instance to type maps
   getTime(1,id,false);
   // reader->createTypes(inputTypeFile,outputTypeDir, workerId, numWorkers);
   createSubjTypeFiles(id,numWorkers, numTypeLines);
   MPI::COMM_WORLD.Gather(&workerStatus,1, MPI_INT, globalStatus, 1, MPI_INT, 0);
   getTime(2,id,true);
   checkStatus(globalStatus,id,1,processes);


   getTime(3,id,false);
   // reader->combineTypeFiles(outputTypeDir, instancePatternDataset, workerId, numWorkers); 
   combineSubjectTypeFiles(id,numWorkers);
   MPI::COMM_WORLD.Barrier();
   getTime(4,id,true);
   checkStatus(globalStatus,id,2,processes);

   
 


   // Stage two create triple files
   getTime(5,id,false);
   // string inputMapFile = "/projects/ExaHDF5/mlewis/wiki/database/mappingbased_properties_en.nt";
   // string inputTripleDir = "/projects/ExaHDF5/mlewis/wiki/database/mapFiles/";
   // reader->createTypeEntries(inputMapFile,instancePatternDataset, inputTripleDir, workerId, numWorkers, numLines);
   createTypeTripleFiles(id,numWorkers,numMapLines);
   MPI::COMM_WORLD.Gather(&workerStatus,1, MPI_INT, globalStatus, 1, MPI_INT, 0);
   getTime(6,id,true);
   checkStatus(globalStatus,id,3,processes);

 

   // Combine all the triple pattern files
   getTime(7,id,false);
   // string inputPatternOffsets = "/projects/ExaHDF5/mlewis/wiki/output/tripleOffsets.txt";
   // string inputTripleDataset = "/projects/ExaHDF5/mlewis/wiki/output/typeTriples.txt";
   // reader->assemblePatternFiles(inputTripleDir,inputTripleDataset, inputPatternOffsets, numWorkers);
   combineTypeTripleFiles(id,numWorkers);
   MPI::COMM_WORLD.Barrier();
   getTime(8,id,true);
   checkStatus(globalStatus,id,4,processes);
   
	
   getTime(9,id,false);
   loadRoots(id,numWorkers);
   MPI::COMM_WORLD.Gather(&workerStatus,1, MPI_INT, globalStatus, 1, MPI_INT, 0);
   getTime(10,id,true);
   checkStatus(globalStatus,id,5,processes);
      
   delete [] timeArray;
   delete [] globalStatus;
   delete workerNode;
   delete reader;
   MPI::Finalize();
   return 1;

   /* MPI::Finalize();
   return 0; */


}

// Create subject-type files 
void createSubjTypeFiles(int processId, int numWorkers, int numLines) {
   if (processId == 0) return;
   int workerId = processId -1;
   // /projects/ExaHDF5/mlewis/wiki/database/instance_types_en.nt-normal
   reader->createTypes(inputTypeFile,outputTypeDir, workerId, numWorkers, numLines);
	workerStatus = 18;
      
}

void combineSubjectTypeFiles(int processId, int numWorkers) {
    int lineSize = 0;
    if (processId == 0) {
       int workerId = processId -1;
       reader->combineTypeFiles(outputTypeDir, instancePatternDataset,numWorkers); 
    }

}

void createTypeTripleFiles(int processId, int numWorkers, int numLines) {
   if (processId == 0) return;
   int workerId = processId -1;
   reader->createTypeEntries(inputMapFile,instancePatternDataset, inputTripleDir, workerId, numWorkers, numLines);
   workerStatus = 16;
}

void combineTypeTripleFiles(int processId, int numWorkers) {
   if (processId == 0) {
      int workerId = processId -1;
      reader->assemblePatternFiles(inputTripleDir,inputTripleDataset, inputPatternOffsets, numWorkers);
   } 
}

void loadRoots(int processId, int numWorkers) {
	if (processId == 0) return;
	int workerId = processId -1;
	workerNode->loadTriples(inputPatternOffsets, inputTripleDataset, workerId, numWorkers);
	workerStatus = 20;
}


void getTime(int stage, int id,bool marker) {
   double totalTime = 0.00000;   
   if (id == 0) {
       ofstream timeFileStream;
       timeFileStream.open(timeFile.c_str(), ofstream::out | ofstream::app);
       gettimeofday(&timeValue,NULL);
       timeArray[stage] = timeValue.tv_sec + (timeValue.tv_usec/10000000);
       if (marker == true) {
         double stageTime = timeArray[stage] - timeArray[stage-1];
         for (int i = 1; i <= stage; i += 2) {
            totalTime += timeArray[i] -timeArray[i-1];
         }

         timeFileStream.precision(5);
         timeFileStream << "Marker: " << stage << " Current time: " << stageTime << " Total time " << totalTime << endl;
       }
       timeFileStream.close();
   }
   
}

void checkStatus(int *stat, int id, int stage, int numWorkers) {
   ofstream fileStream;
   if (id == 0) {
     // fileStream.open(debugFile.c_str(),ofstream::out | ofstream::app);
      cout << " STAGE " << stage << ": ";
      for (int i = 0; i < numWorkers; i++ ) {
	 if (stat) cout << stat[i] << " ";
      }
      cout << endl;
      
      // fileStream << endl;
      // fileStream.close();
   }
}

