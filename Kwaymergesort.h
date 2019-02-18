//  Kwaymergesort.h
//  Created by Sina Pilehchiha on 2019-02-09.
//  Copyright © 2019 TeamNumber02. All rights reserved.

#ifndef KWAYMERGESORT_H
#define KWAYMERGESORT_H

//  includes
#include <iostream> //  This header is part of the Input/output library.
#include <fstream>  //   This header is also part of the Input/Output library.
#include <sstream>  //   This header is, too, part of the Input/Output library.
#include <queue>    // This header is part of the containers library.
#include <libgen.h> //  This header concerns the basename() function
#include <string>
//  below, there is a basic struct for a CLAIM entry.
//  a CLAIM tuple's structure gets defined as a new data structure.
struct CLAIM {
    
    //  The following two variable are actually the only attributes from the CLAIM relation that the sorter needs
    //  to process the designated query in the project description file.
    //   data
    char clientID[10];
    char compensationAmount[13];
    
    //   overload the < (less than) operator for comparison between CLAIM records.
    bool operator < (const CLAIM &claim) const
    {
        //  first try to compare clinetIDs in char format.
        //  if equal, compare their compensationAmounts.
        if (std::strcmp(clientID, claim.clientID) != 0) {
            
            //    convert clientID character arrays into integers in order to compare their values.
            if      (atoi(clientID) < atoi(claim.clientID))  return true;
            else if (atoi(clientID) > atoi(claim.clientID))  return false;
        }
        
        //  the sorter gets here when clientIDs are the same. now it tries to sort based on compensationAmounts.
        //  convert compensationAmount character arrays into floats in order to compare their values.
        if      (atof(compensationAmount) < atof(claim.compensationAmount))  return true;
        else return false;
    }
    
    //    overload the << operator for writing a CLAIM record.
    friend std::ostream& operator<<(std::ostream &os, const CLAIM &claim)
    {
        //  The reason for inserting whitespace into the output stream is to retain the initial data format from the input file.
        os << std::string(18, ' ') << claim.clientID << std::string(214, ' ') << claim.compensationAmount;
        return os;
    }
    
    //  overload the >> operator for reading into a CLAIM record.
    friend std::istream& operator>>(std::istream &is, CLAIM &claim)
    {
        is.ignore(18); //   ignore claimNumber and claimDate.
        is.get(claim.clientID, 10);
        is.ignore(214); //  ignore clientName, clientAddress, clientEmailAddress, insuredItemID, and damageAmount.
        is.get(claim.compensationAmount, 13);
        is.ignore(1); // ignore the whitespace character at the end of each line of input
        return is;
    }
};

//  the datatype struct used by the priority_queue
struct CLAIM_SCHEMA {
    
    CLAIM datum; //  data
    std::istream* stream;
    bool (*comparisonFunction)(const CLAIM &c1, const CLAIM &c2);
    CLAIM_SCHEMA (const CLAIM &datum, //    constructor
                  std::istream* stream,
                  bool (*comparisonFunction)(const CLAIM &c1, const CLAIM &c2))
    :
    datum(datum),
    stream(stream),
    comparisonFunction(comparisonFunction) {}
    
    bool operator < (const CLAIM_SCHEMA &c) const
    {
        //  recall that priority queues try to sort from highest to lowest. thus, we need to negate.
        return !(comparisonFunction(datum, c.datum));
    }
};

struct KwayMergeSort {
    KwayMergeSort(const std::string &inFile, // constructor, using CLAIM's overloaded < operator.  Must be defined.
                  const std::string &outFile,
                  const std::string  &maxBufferSize,
                  std::string tempPath,
                  bool (*compareFunction)(const CLAIM &c1, const CLAIM &c2) = NULL);
    
    ~KwayMergeSort(void); //    destructor
    
    void Sort(); // Sort the data
    void SetComparison(bool (*compareFunction)(const CLAIM &c1, const CLAIM &c2));   // change the sort criteria
    
    std::string _inFile;
    bool (*_compareFunction)(const CLAIM &c1, const CLAIM &c2);
    std::string _tempPath;
    std::vector<std::string> _vTempFileNames;
    std::vector<std::ifstream*> _vTempFiles;
    std::string _maxBufferSize;
    unsigned int _runCounter;
    std::string _outFile;
    void DivideAndSort(); //    drives the creation of sorted sub-files stored on disk.
    void Merge(); //    drives the merging of the sorted temp files.
    void WriteToTempFile(const std::vector<CLAIM> &lines); //   final, sorted and merged output is written to an output file.
    void OpenTempFiles();
    void CloseTempFiles();
    void sumOfCompensationAmounts();
    void showTopTenCostlyClients();
};
KwayMergeSort::KwayMergeSort (const std::string &inFile, // constructor
                              const std::string &outFile,
                              const std::string  &maxBufferSize,
                              std::string tempPath,
                              bool (*compareFunction)(const CLAIM &c1, const CLAIM &c2))
: _inFile(inFile)
, _outFile(outFile)
, _tempPath(tempPath)
, _maxBufferSize(maxBufferSize)
, _compareFunction(compareFunction)
, _runCounter(0) {}

KwayMergeSort::~KwayMergeSort(void) {} //   destructor

void KwayMergeSort::Sort() { // API for sorting.
    DivideAndSort();
    Merge();
}

// change the sorting criteria
void KwayMergeSort::SetComparison (bool (*compareFunction)(const CLAIM &c1, const CLAIM &c2)) {
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

void KwayMergeSort::OpenTempFiles() {
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
            exit(1);
        }
    }
}

void KwayMergeSort::CloseTempFiles() {
    for (size_t i=0; i < _vTempFiles.size(); ++i) { //  delete the pointers to the temp files.
        _vTempFiles[i]->close();
        delete _vTempFiles[i];
    }
    for (size_t i=0; i < _vTempFileNames.size(); ++i) { //  delete the temp files from the file system.
        remove(_vTempFileNames[i].c_str());  // remove = UNIX "rm"
    }
}

void KwayMergeSort::WriteToTempFile(const std::vector<CLAIM> &lineBuffer) {
    std::stringstream tempFileSS; //    name the current tempfile
    if (_tempPath.size() == 0)
        tempFileSS << _inFile << "." << _runCounter;
    else
        tempFileSS << _tempPath << "/" << stl_basename(_inFile) << "." << _runCounter;
    std::string tempFileName = tempFileSS.str();
    std::ofstream* output;
    output = new std::ofstream(tempFileName.c_str(), std::ios::out);
    for (size_t i = 0; i < lineBuffer.size(); ++i) { // write the contents of the current buffer to the temp file
        *output << lineBuffer[i] << std::endl;
    }
    ++_runCounter; //   update the tempFile number and add the tempFile to the list of tempFiles
    output->close();
    delete output;
    _vTempFileNames.push_back(tempFileName);
}

void KwayMergeSort::DivideAndSort() {
    std::istream* input = new std::ifstream(_inFile.c_str(), std::ios::in);
    std::vector<CLAIM> lineBuffer;
    lineBuffer.reserve(stoi(_maxBufferSize));
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.
    CLAIM line;
    while (*input >> line) { // keep reading until there is no more input data
        lineBuffer.push_back(line); //  add the current line to the buffer and
        //totalBytes += sizeof(line);
        totalBytes += sizeof(lineBuffer.size()); //  track the memory used.
        if (totalBytes > stoi(_maxBufferSize) - sizeof(line)) { //    sort the buffer and write to a temp file if we have filled up our quota
            if (_compareFunction != NULL)
                sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
            else
                sort(lineBuffer.begin(), lineBuffer.end()); //  sort the buffer.
            WriteToTempFile(lineBuffer); // write the sorted data to a temp file
            lineBuffer.clear(); //  clear the buffer for the next run
            totalBytes = 0; // make the totalBytes counter zero in order to count the bytes occupying the buffer again.
        }
    }
    if (lineBuffer.empty() == false) {  //  handle the run (if any) from the last chunk of the input file.
        if (_compareFunction != NULL)
            sort(lineBuffer.begin(), lineBuffer.end(), *_compareFunction);
        else
            sort(lineBuffer.begin(), lineBuffer.end());
        WriteToTempFile(lineBuffer); // write the sorted data to a temp file
    }
}

void KwayMergeSort::Merge() { //    Merge the sorted temp files.
    // uses a priority queue, with the values being a pair of the record from the file, and the stream from which the record came
    // open the sorted temp files up for merging.
    // loads ifstream pointers into _vTempFiles
    std::ostream *output = new std::ofstream(_outFile.c_str(), std::ios::out);
    OpenTempFiles();
    
    //  A priority queue is a container adaptor
    //  that provides constant time lookup of the largest (by default) element,
    //  at the expense of logarithmic insertion and extraction.
    std::priority_queue<CLAIM_SCHEMA> outQueue; //  priority queue for the buffer.
    CLAIM line; //  extract the first line from each temp file
    for (size_t i = 0; i < _vTempFiles.size(); ++i) {
        *_vTempFiles[i] >> line;
        outQueue.push(CLAIM_SCHEMA(line, _vTempFiles[i], _compareFunction));
    }
    while (outQueue.empty() == false) { //  keep working until the queue is empty
        CLAIM_SCHEMA lowest = outQueue.top();  //   grab the lowest element, print it, then ditch it.
        *output << lowest.datum << std::endl; //    write the entry from the top of the queue
        outQueue.pop(); //  remove this record from the queue
        *(lowest.stream) >> line; //    add the next line from the lowest stream (above) to the queue as long as it's not EOF.
        if (*(lowest.stream))
            outQueue.push( CLAIM_SCHEMA(line, lowest.stream, _compareFunction) );
    }
    CloseTempFiles();  //   clean up the temp files.
}

void KwayMergeSort::sumOfCompensationAmounts() {
    std::istream* input  = new std::ifstream(_outFile.c_str(), std::ios::in);
    std::ofstream sumOfCompensationAmountsFile;
    sumOfCompensationAmountsFile.open("sumOfCompensationAmountsFile.txt");
    CLAIM line0, line;
    *input >> line0;
    while (*input >> line) { // keep reading until there is no more input data
        if (std::string(line0.clientID) == std::string(line.clientID)) {
            sprintf(line0.compensationAmount, "%.2f", atof(line0.compensationAmount) + atof(line.compensationAmount));
        }
        else {
            sumOfCompensationAmountsFile << line0 << std::endl;
            line0 = line;
        }
    }
    sumOfCompensationAmountsFile << line0 << std::endl;;
    sumOfCompensationAmountsFile.close();}

void KwayMergeSort::showTopTenCostlyClients() {
    const std::string outFile2;
    std::istream* input = new std::ifstream(_outFile.c_str(), std::ios::in);
    CLAIM line;
    std::cout << "Client ID" << "\t" << "Sum of Compensation Amount\n\n";
    for(unsigned short i = 0; i < 10; i++) {
        *input >> line;
        std::cout << line.clientID << "\t" << line.compensationAmount << std::endl;
    }
}
#endif /* KWAYMERGESORT_H */
