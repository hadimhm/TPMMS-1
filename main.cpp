// TPMMS (Two Pass Multiway Merge Sort) Algorithm Implementation
// Written by Sina Pilehchiha [sina.pilehchiha@mail.concordia.ca]
// January - February 2019

#include <iostream> // This header is part of the Input/output library.
#include <string>
#include <cstdlib>
#include <fstream> // This header is also part of the Input/Output library.
#include <vector>
#include <math.h>
#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include <sstream> // This header is, too, part of the Input/Output library.
#include <queue> // This header is part of the containers library.
#include <cstdio>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <libgen.h> // This header concerns the basename() function.
#include <sys/resource.h>
#include <sys/time.h>

// A Claim record with its attributes is defined as a new data type.
// This data struct is used for the first pass of the algorithm.
struct Claim {
    
    char ClaimNumber[9];
    char ClaimDate[11];
    char clientID[10];
    char clientName[26];
    char clientAddress[151];
    char clientEmailAddress[29];
    char insuredItemID[3];
    char damageAmount[10];
    char compensationAmount[19];
    
    //   overload the < (less than) operator for comparison between Claim records.
    bool operator < (const Claim &Claim) const
    {
        //  first try to compare clinetIDs in char format.
        //  if equal, compare their compensationAmounts.
        if (std::strcmp(clientID, Claim.clientID) != 0) {
            
            //    convert clientID character arrays into integers in order to compare their values.
            if      (atoi(clientID) < atoi(Claim.clientID))  return true;
            else if (atoi(clientID) > atoi(Claim.clientID))  return false;
        }
        
        //  the sorter gets here when clientIDs are the same. now it tries to sort based on compensationAmounts.
        //  convert compensationAmount character arrays into floats in order to compare their values.
        if      (atof(compensationAmount) < atof(Claim.compensationAmount))  return true;
        else return false;
    }
    
    //    overload the << operator for writing a Claim record.
    friend std::ostream& operator<<(std::ostream &os, const Claim &Claim)
    {
        //  The reason for inserting whitespace into the output stream is to retain the initial data format from the input file.
        os << Claim.ClaimNumber << Claim.ClaimDate << Claim.clientID << Claim.clientName << Claim.clientAddress << Claim.clientEmailAddress << Claim.insuredItemID << Claim.damageAmount << Claim.compensationAmount;
        return os;
    }
    
    //  overload the >> operator for reading into a Claim record.
    friend std::istream& operator>>(std::istream &is, Claim &Claim)
    {
        is.get(Claim.ClaimNumber, 9);
        is.get(Claim.ClaimDate, 11);
        is.get(Claim.clientID, 10);
        is.get(Claim.clientName, 26);
        is.get(Claim.clientAddress, 151);
        is.get(Claim.clientEmailAddress, 29);
        is.get(Claim.insuredItemID, 3);
        is.get(Claim.damageAmount, 10);
        is.get(Claim.compensationAmount, 19);
        is.ignore(1); // ignore the whitespace character at the end of each record of input
        return is;
    }
};

//  the datatype struct used by the priority_queue
struct Claim_SCHEMA {
    
    Claim datum; //  data
    std::istream* stream;
    bool (*comparisonFunction)(const Claim &c1, const Claim &c2);
    Claim_SCHEMA (const Claim &datum, //    constructor
                  std::istream* stream,
                  bool (*comparisonFunction)(const Claim &c1, const Claim &c2))
    :
    datum(datum),
    stream(stream),
    comparisonFunction(comparisonFunction) {}
    
    bool operator < (const Claim_SCHEMA &c) const
    {
        //  recall that priority queues try to sort from highest to lowest. thus, we need to negate.
        return !(comparisonFunction(datum, c.datum));
    }
};

bool byClientID(Claim const &c1, Claim const &c2) {
    if (std::strcmp(c1.clientID, c2.clientID) != 0) {
        
        //    convert clientID character arrays into integers in order to compare their values.
        if      (atoi(c1.clientID) < atoi(c2.clientID))  return true;
        else if (atoi(c1.clientID) > atoi(c2.clientID))  return false;
    }
    
    //  the sorter gets here when clientIDs are the same. now it tries to sort based on compensationAmounts.
    //  convert compensationAmount character arrays into floats in order to compare their values.
    if      (atof(c1.compensationAmount) < atof(c2.compensationAmount))  return true;
    else return false;
}

// comparison function for sorting by chromosome, then by start.
bool byCompensationAmount(Claim const &c1, Claim const &c2) {
    return (atof(c1.compensationAmount) > atof(c2.compensationAmount));
}

struct TPMMS {
    TPMMS(const std::string &inFile, // constructor, using Claim's overloaded < operator.  Must be defined.
          const std::string &outFile,
          const std::string  &maxBufferSize,
          std::string tempPath,
          bool (*compareFunction)(const Claim &c1, const Claim &c2) = NULL);
    inline void* operator new(size_t);
    inline void   operator delete(void*);
    
    ~TPMMS(void); //    destructor
    
    void Sort(); // Sort the data
    void SetComparison(bool (*compareFunction)(const Claim &c1, const Claim &c2));   // change the sort criteria
    
    std::string _inFile;
    bool (*_compareFunction)(const Claim &c1, const Claim &c2);
    std::string _tempPath;
    std::vector<std::string> _vTempFileNames;
    std::vector<std::ifstream*> _vTempFiles;
    std::string _maxBufferSize;
    unsigned int _runCounter;
    std::string _outFile;
    void Pass1(); //    drives the creation of sorted sub-files stored on disk.
    void Pass2(); //    drives the merging of the sorted temp files.
    void WriteToTempFile(const std::vector<Claim> &lines); //   final, sorted and merged output is written to an output file.
    void OpenTempFiles();
    void CloseTempFiles();
    void SumOfCompensationAmounts();
    void ShowTopTenCostliestClients();
};
TPMMS::TPMMS (const std::string &inFile, // constructor
              const std::string &outFile,
              const std::string  &maxBufferSize,
              std::string tempPath,
              bool (*compareFunction)(const Claim &c1, const Claim &c2))
: _inFile(inFile)
, _outFile(outFile)
, _tempPath(tempPath)
, _maxBufferSize(maxBufferSize)
, _compareFunction(compareFunction)
, _runCounter(0) {}

TPMMS::~TPMMS(void) {} //   destructor

void TPMMS::Sort() { // API for sorting.
    Pass1();
    Pass2();
}

// change the sorting criteria
void TPMMS::SetComparison (bool (*compareFunction)(const Claim &c1, const Claim &c2)) {
    _compareFunction = compareFunction;
}

std::string stl_basename(const std::string &path) { //   STLized version of basename() (because POSIX basename() modifies the input string pointer.)
    std::string result;
    char* path_dup = strdup(path.c_str());
    char* basename_part = basename(path_dup);
    result = basename_part;
    free(path_dup);
    size_t pos = result.find_last_of('.');
    if (pos != std::string::npos ) // checks whether pos is at the end of the sting or not.
        result = result.substr(0,pos); // updates the length of result.
    return result;
}

void TPMMS::OpenTempFiles() {
    for (size_t i=0; i < _vTempFileNames.size(); ++i) {
        std::ifstream* file;
        file = new std::ifstream(_vTempFileNames[i].c_str(), std::ios::in);
        if (file->good() == true) {
            _vTempFiles.push_back(file); // add a pointer to the opened temp file to the list
        }
        else {
            std::cerr << "Unable to open temp file (" << _vTempFileNames[i]
            << ").  I suspect a limit on number of open file handles.  Exiting."
            << std::endl;
            CloseTempFiles();
            exit(1);
        }
    }
}

void TPMMS::CloseTempFiles() {
    for (size_t i=0; i < _vTempFiles.size(); ++i) { //  delete the pointers to the temp files.
        _vTempFiles[i]->close();
        delete _vTempFiles[i];
    }
    for (size_t i=0; i < _vTempFileNames.size(); ++i) { //  delete the temp files from the file system.
        remove(_vTempFileNames[i].c_str());  // remove = UNIX "rm"
    }
}

void TPMMS::WriteToTempFile(const std::vector<Claim> &buffer) {
    std::stringstream tempFileSS; //    name the current tempfile
    if (_tempPath.size() == 0)
        tempFileSS << _inFile << "." << _runCounter;
    else
        tempFileSS << _tempPath << "/" << stl_basename(_inFile) << "." << _runCounter;
    std::string tempFileName = tempFileSS.str();
    std::ofstream* output;
    output = new std::ofstream(tempFileName, std::ios::out);
    for (size_t i = 0; i < buffer.size(); ++i) { // write the contents of the current buffer to the temp file
        *output << buffer[i] << std::endl;
    }
    ++_runCounter; //   update the tempFile number and add the tempFile to the list of tempFiles
    output->close();
    delete output;
    _vTempFileNames.push_back(tempFileName);
}

void TPMMS::Pass1() {
    std::istream* input = new std::ifstream(_inFile.c_str(), std::ios::in);
    std::vector<Claim> buffer;
    buffer.reserve(stoi(_maxBufferSize));
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.
    Claim record;
    while (*input >> record) { // keep reading until there is no more input data
        buffer.push_back(record); //  add the current record to the buffer and
        //totalBytes += sizeof(line);
        totalBytes += sizeof(record); //  track the memory used.
        if (totalBytes > (stoi(_maxBufferSize) * sizeof(record)) - sizeof(record)) { //    sort the buffer and write to a temp file if we have filled up our quota
            if (_compareFunction != NULL)
                sort(buffer.begin(), buffer.end(), *_compareFunction);
            else
                sort(buffer.begin(), buffer.end()); //  sort the buffer.
            WriteToTempFile(buffer); // write the sorted data to a temp file
            buffer.clear(); //  clear the buffer for the next run
            totalBytes = 0; // make the totalBytes counter zero in order to count the bytes occupying the buffer again.
        }
    }
    if (buffer.empty() == false) {  //  handle the run (if any) from the last chunk of the input file.
        if (_compareFunction != NULL)
            sort(buffer.begin(), buffer.end(), *_compareFunction);
        else
            sort(buffer.begin(), buffer.end());
        WriteToTempFile(buffer); // write the sorted data to a temp file
    }
}

void TPMMS::Pass2() { //    Merge the sorted temp files.
    // uses a priority queue, with the values being a pair of the record from the file, and the stream from which the record came
    // open the sorted temp files up for merging.
    // loads ifstream pointers into _vTempFiles
    std::ostream *output = new std::ofstream(_outFile.c_str(), std::ios::out);
    OpenTempFiles();
    
    //  A priority queue is a container adaptor
    //  that provides constant time lookup of the largest (by default) element,
    //  at the expense of logarithmic insertion and extraction.
    std::priority_queue<Claim_SCHEMA> outQueue; //  priority queue for the buffer.
    Claim record; //  extract the first record from each temp file
    for (size_t i = 0; i < _vTempFiles.size(); ++i) {
        *_vTempFiles[i] >> record;
        outQueue.push(Claim_SCHEMA(record, _vTempFiles[i], _compareFunction));
    }
    while (outQueue.empty() == false) { //  keep working until the queue is empty
        Claim_SCHEMA lowest = outQueue.top();  //   grab the lowest element, print it, then ditch it.
        *output << lowest.datum << std::endl; //    write the entry from the top of the queue
        outQueue.pop(); //  remove this record from the queue
        *(lowest.stream) >> record; //    add the next record from the lowest stream (above) to the queue as long as it's not EOF.
        if (*(lowest.stream))
            outQueue.push( Claim_SCHEMA(record, lowest.stream, _compareFunction) );
    }
    CloseTempFiles();  //   clean up the temp files.
}

void TPMMS::SumOfCompensationAmounts() {
    std::istream* input  = new std::ifstream(_outFile.c_str(), std::ios::in);
    std::ofstream SumOfCompensationAmountsFile;
    SumOfCompensationAmountsFile.open("SumOfCompensationAmountsFile.txt");
    Claim initialRecord, record;
    *input >> initialRecord;
    while (*input >> record) { // keep reading until there is no more input data
        if (std::string(initialRecord.clientID) == std::string(record.clientID)) {
            sprintf(initialRecord.compensationAmount, "%.2f", atof(initialRecord.compensationAmount) + atof(record.compensationAmount));
        }
        else {
            SumOfCompensationAmountsFile << initialRecord << std::endl;
            initialRecord = record;
        }
    }
    SumOfCompensationAmountsFile << initialRecord << std::endl;;
    SumOfCompensationAmountsFile.close();}

void TPMMS::ShowTopTenCostliestClients() {
    const std::string outFile2;
    std::istream* input = new std::ifstream(_outFile.c_str(), std::ios::in);
    Claim record;
    std::cout << "Client ID" << "\t" << "Sum of Compensation Amount\n\n";
    for(unsigned short i = 0; i < 10; i++) {
        *input >> record;
        std::cout << record.clientID << "\t" << record.compensationAmount << std::endl;
    }
}
// A program shall contain a global function named main, which is the designated start of the program.
int main(int argc, char* argv[]) {
    // This argument is given to the executable pogram via the command line interface.
    std::string inputFile = argv[1];
    
    // Allow the sorter to use an arbitrary amount (in bytes) of memory for sorting.
    std::string bufferSize = argv[2];
    
    // Once the buffer is full, the sorter will dump the buffer's content to a temporary file and grab another chunk from the input file.
    std::string temporaryPath = argv[3]; // Allows you to write the intermediate files anywhere you want.
    
    const clock_t BEGINNING = clock(); // Mark the beginning of the execution of the sorting procedure.
    
    // Create a new instance of the TPMMS class.
    TPMMS* firstSorter = new TPMMS (inputFile, "outputFile.txt", bufferSize, temporaryPath, byClientID) ;
    firstSorter->Sort();
    firstSorter->SumOfCompensationAmounts();
    
    TPMMS* secondSorter = new TPMMS ("SumOfCompensationAmountsFile.txt", "outputFile2.txt", bufferSize, temporaryPath, byCompensationAmount);
    secondSorter->Sort();
    
    const double EXECUTION_TIME = (double)(clock() - BEGINNING) / CLOCKS_PER_SEC; // Report the execution time (in minutes).
    
    secondSorter->ShowTopTenCostliestClients();
    
    std::cout << "\n" << "Execution time in seconds:\t" << EXECUTION_TIME << "\n"; // Print out the time elapsed sorting.
}
